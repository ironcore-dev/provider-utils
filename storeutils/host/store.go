// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	utilssync "github.com/ironcore-dev/provider-utils/storeutils/sync"
	"github.com/ironcore-dev/provider-utils/storeutils/utils"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
)

type Options[E api.Object] struct {
	Dir             string
	NewFunc         func() E
	CreateStrategy  CreateStrategy[E]
	WatchBufferSize int
	FieldIndexers   map[string]store.IndexerFunc[E]
}

func (o *Options[E]) Defaults() {
	if o.WatchBufferSize <= 0 {
		o.WatchBufferSize = 20
	}
}

func NewStore[E api.Object](opts Options[E]) (*Store[E], error) {
	opts.Defaults()

	if opts.NewFunc == nil {
		return nil, fmt.Errorf("must specify opts.NewFunc")
	}

	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating store directory: %w", err)
	}

	indexers := make(map[string]store.IndexerFunc[E], len(opts.FieldIndexers))
	for k, v := range opts.FieldIndexers {
		indexers[k] = v
	}

	s := &Store[E]{
		dir: opts.Dir,

		idMu: utilssync.NewMutexMap[string](),

		newFunc:        opts.NewFunc,
		createStrategy: opts.CreateStrategy,

		watches:         sets.New[*watch[E]](),
		watchBufferSize: opts.WatchBufferSize,

		indexers:   indexers,
		labelIndex: make(map[string]map[string]sets.Set[string]),
		fieldIndex: make(map[string]map[string]sets.Set[string]),
	}

	entries, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("error reading store directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		obj, err := s.get(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("error warming up index: %w", err)
		}
		s.addToIndex(obj)
	}

	return s, nil
}

type Store[E api.Object] struct {
	dir string

	idMu *utilssync.MutexMap[string]

	newFunc         func() E
	createStrategy  CreateStrategy[E]
	watchBufferSize int
	watchesMu       sync.RWMutex
	watches         sets.Set[*watch[E]]

	indexers map[string]store.IndexerFunc[E]

	indexMu    sync.RWMutex
	labelIndex map[string]map[string]sets.Set[string]
	fieldIndex map[string]map[string]sets.Set[string]
}

type CreateStrategy[E api.Object] interface {
	PrepareForCreate(obj E)
}

func (s *Store[E]) addToIndex(obj E) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	id := obj.GetID()
	for k, v := range obj.GetLabels() {
		if s.labelIndex[k] == nil {
			s.labelIndex[k] = make(map[string]sets.Set[string])
		}
		if s.labelIndex[k][v] == nil {
			s.labelIndex[k][v] = sets.New[string]()
		}
		s.labelIndex[k][v].Insert(id)
	}
	for field, fn := range s.indexers {
		v := fn(obj)
		if s.fieldIndex[field] == nil {
			s.fieldIndex[field] = make(map[string]sets.Set[string])
		}
		if s.fieldIndex[field][v] == nil {
			s.fieldIndex[field][v] = sets.New[string]()
		}
		s.fieldIndex[field][v].Insert(id)
	}
}

func (s *Store[E]) removeFromIndex(obj E) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	id := obj.GetID()
	for k, v := range obj.GetLabels() {
		if m := s.labelIndex[k]; m != nil {
			m[v].Delete(id)
		}
	}
	for field, fn := range s.indexers {
		v := fn(obj)
		if m := s.fieldIndex[field]; m != nil {
			m[v].Delete(id)
		}
	}
}

// resolveCandidateIDs returns the set of IDs matching all positive index-backed
// requirements. Returns nil when no filters are present (full scan needed).
// Negative label requirements (NotIn, DoesNotExist) are skipped here and
// applied as a post-filter in List.
func (s *Store[E]) resolveCandidateIDs(opts *store.ListOptions) sets.Set[string] {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	var result sets.Set[string]
	hasFilter := false

	if opts.LabelSelector != nil {
		reqs, selectable := opts.LabelSelector.Requirements()
		if !selectable {
			return sets.New[string]()
		}
		for _, req := range reqs {
			switch req.Operator() {
			case selection.Equals, selection.DoubleEquals, selection.In:
				hasFilter = true
				ids := sets.New[string]()
				if m := s.labelIndex[req.Key()]; m != nil {
					for v := range req.Values() {
						if s := m[v]; s != nil {
							ids = ids.Union(s)
						}
					}
				}
				result = indexIntersect(result, ids)
			case selection.Exists:
				hasFilter = true
				ids := sets.New[string]()
				if m := s.labelIndex[req.Key()]; m != nil {
					for _, s := range m {
						ids = ids.Union(s)
					}
				}
				result = indexIntersect(result, ids)
			}
		}
	}

	if opts.FieldSelector != nil {
		for _, req := range opts.FieldSelector.Requirements() {
			hasFilter = true
			ids := sets.New[string]()
			if m := s.fieldIndex[req.Field]; m != nil {
				if s := m[req.Value]; s != nil {
					ids = s.Union(sets.New[string]())
				}
			}
			result = indexIntersect(result, ids)
		}
	}

	if !hasFilter {
		return nil
	}
	if result == nil {
		return sets.New[string]()
	}
	return result
}

// indexIntersect intersects a and b. A nil a means "universe" — returns a copy of b.
func indexIntersect(a, b sets.Set[string]) sets.Set[string] {
	if a == nil {
		return b.Union(sets.New[string]())
	}
	return a.Intersection(b)
}

func (s *Store[E]) Create(_ context.Context, obj E) (E, error) {
	s.idMu.Lock(obj.GetID())
	defer s.idMu.Unlock(obj.GetID())

	_, err := s.get(obj.GetID())
	switch {
	case err == nil:
		return utils.Zero[E](), fmt.Errorf("object with id %q %w", obj.GetID(), store.ErrAlreadyExists)
	case errors.Is(err, store.ErrNotFound):
	default:
		return utils.Zero[E](), fmt.Errorf("failed to get object with id %q %w", obj.GetID(), err)
	}

	if s.createStrategy != nil {
		s.createStrategy.PrepareForCreate(obj)
	}

	obj.SetCreatedAt(time.Now())
	obj.IncrementResourceVersion()

	obj, err = s.set(obj)
	if err != nil {
		return utils.Zero[E](), err
	}

	s.addToIndex(obj)

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeCreated,
		Object: obj,
	})

	return obj, nil
}

func (s *Store[E]) Get(_ context.Context, id string) (E, error) {
	s.idMu.Lock(id)
	defer s.idMu.Unlock(id)

	object, err := s.get(id)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to read object: %w", err)
	}

	return object, nil
}

func (s *Store[E]) Update(_ context.Context, obj E) (E, error) {
	s.idMu.Lock(obj.GetID())
	defer s.idMu.Unlock(obj.GetID())

	oldObj, err := s.get(obj.GetID())
	if err != nil {
		return utils.Zero[E](), err
	}

	if obj.GetDeletedAt() != nil && len(obj.GetFinalizers()) == 0 {
		if err := s.delete(obj); err != nil {
			return utils.Zero[E](), fmt.Errorf("failed to delete object metadata: %w", err)
		}
		return obj, nil
	}

	if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
		return utils.Zero[E](), fmt.Errorf("failed to update object: %w", store.ErrResourceVersionNotLatest)
	}

	if reflect.DeepEqual(oldObj, obj) {
		return obj, nil
	}

	obj.IncrementResourceVersion()

	obj, err = s.set(obj)
	if err != nil {
		return utils.Zero[E](), err
	}

	s.removeFromIndex(oldObj)
	s.addToIndex(obj)

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeUpdated,
		Object: obj,
	})

	return obj, nil
}

func (s *Store[E]) Delete(_ context.Context, id string) error {
	s.idMu.Lock(id)
	defer s.idMu.Unlock(id)

	obj, err := s.get(id)
	if err != nil {
		return err
	}

	if len(obj.GetFinalizers()) == 0 {
		return s.delete(obj)
	}

	if obj.GetDeletedAt() != nil {
		return nil
	}

	now := time.Now()
	obj.SetDeletedAt(&now)
	obj.IncrementResourceVersion()

	if _, err := s.set(obj); err != nil {
		return fmt.Errorf("failed to set object metadata: %w", err)
	}

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeDeleted,
		Object: obj,
	})

	return nil
}

func (s *Store[E]) List(ctx context.Context, opts ...store.ListOption) ([]E, error) {
	listOpts := &store.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	if listOpts.FieldSelector != nil {
		for _, req := range listOpts.FieldSelector.Requirements() {
			if _, ok := s.indexers[req.Field]; !ok {
				return nil, fmt.Errorf("field selector references unindexed field %q", req.Field)
			}
		}
	}

	candidateIDs := s.resolveCandidateIDs(listOpts)

	if candidateIDs != nil {
		var objs []E
		for _, id := range candidateIDs.UnsortedList() {
			obj, err := s.Get(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("failed to read object: %w", err)
			}
			if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
				continue
			}
			objs = append(objs, obj)
		}
		return objs, nil
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	//nolint:prealloc
	var objs []E
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		object, err := s.Get(ctx, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to read object: %w", err)
		}

		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labels.Set(object.GetLabels())) {
			continue
		}

		objs = append(objs, object)
	}

	return objs, nil
}

func (s *Store[E]) Watch(_ context.Context) (store.Watch[E], error) {
	s.watchesMu.Lock()
	defer s.watchesMu.Unlock()

	w := &watch[E]{
		store:  s,
		events: make(chan store.WatchEvent[E], s.watchBufferSize),
	}

	s.watches.Insert(w)

	return w, nil
}

func (s *Store[E]) get(id string) (E, error) {
	file, err := os.ReadFile(filepath.Join(s.dir, id))
	if err != nil {
		if !os.IsNotExist(err) {
			return utils.Zero[E](), fmt.Errorf("failed to read file: %w", err)
		}

		return utils.Zero[E](), fmt.Errorf("object with id %q %w", id, store.ErrNotFound)
	}

	obj := s.newFunc()
	if err := json.Unmarshal(file, &obj); err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to unmarshal object from file %s: %w", id, err)
	}

	return obj, err
}

func (s *Store[E]) set(obj E) (E, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to marshal obj: %w", err)
	}

	if err := os.WriteFile(filepath.Join(s.dir, obj.GetID()), data, 0666); err != nil {
		return utils.Zero[E](), nil
	}

	return obj, nil
}

func (s *Store[E]) delete(obj E) error {
	s.removeFromIndex(obj)

	if err := os.Remove(filepath.Join(s.dir, obj.GetID())); err != nil {
		return fmt.Errorf("failed to delete object from store: %w", err)
	}

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeDeleted,
		Object: obj,
	})

	return nil
}

func (s *Store[E]) watchHandlers() []*watch[E] {
	s.watchesMu.RLock()
	defer s.watchesMu.RUnlock()

	return s.watches.UnsortedList()
}

func (s *Store[E]) enqueue(evt store.WatchEvent[E]) {
	for _, handler := range s.watchHandlers() {
		select {
		case handler.events <- evt:
		default:
		}
	}
}
