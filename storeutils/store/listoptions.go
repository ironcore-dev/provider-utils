// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

type ListOptions struct {
	LabelSelector labels.Selector
}

type ListOption interface {
	ApplyToList(*ListOptions)
}

type MatchingLabels labels.Set

func (m MatchingLabels) ApplyToList(o *ListOptions) {
	sel := labels.SelectorFromSet(labels.Set(m))
	o.LabelSelector = andSelectors(o.LabelSelector, sel)
}

type HasLabels []string

func (h HasLabels) ApplyToList(o *ListOptions) {
	sel := labels.NewSelector()
	for _, key := range h {
		req, _ := labels.NewRequirement(key, selection.Exists, nil)
		sel = sel.Add(*req)
	}
	o.LabelSelector = andSelectors(o.LabelSelector, sel)
}

func andSelectors(existing, additional labels.Selector) labels.Selector {
	if existing == nil {
		return additional
	}
	reqs, _ := additional.Requirements()
	return existing.Add(reqs...)
}
