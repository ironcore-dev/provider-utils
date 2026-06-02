// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package host_test

import (
	"context"
	"os"
	"path/filepath"

	"github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func createDummy(ctx context.Context, s store.Store[*Dummy], id string, labels map[string]string, spec string) *Dummy {
	obj, err := s.Create(ctx, &Dummy{
		Metadata: api.Metadata{ID: id, Labels: labels},
		Spec:     spec,
	})
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(s.Delete, ctx, id)
	return obj
}

var _ = Describe("Store", func() {

	It("should correctly create a object", func(ctx SpecContext) {
		By("creating a watch")
		watch, err := dummyStore.Watch(ctx)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(watch.Stop)

		By("creating a object")
		obj, err := dummyStore.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID: "test-id",
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).NotTo(BeNil())
		DeferCleanup(dummyStore.Delete, ctx, "test-id")

		By("checking that the store object exists")
		data, err := os.ReadFile(filepath.Join(tmpDir, obj.ID))
		Expect(err).NotTo(HaveOccurred())
		Expect(data).NotTo(BeNil())

		By("checking that the event got fired")
		event := &store.WatchEvent[*Dummy]{
			Type:   store.WatchEventTypeCreated,
			Object: obj,
		}
		Eventually(watch.Events()).Should(Receive(event))
	})

	It("should filter objects by labels using MatchingLabels", func(ctx SpecContext) {
		By("creating objects with different labels")
		createDummy(ctx, dummyStore, "labeled-a", map[string]string{"app": "foo", "env": "prod"}, "")
		createDummy(ctx, dummyStore, "labeled-b", map[string]string{"app": "bar", "env": "prod"}, "")
		createDummy(ctx, dummyStore, "labeled-c", map[string]string{"app": "foo", "env": "dev"}, "")

		By("listing without filter returns all objects")
		all, err := dummyStore.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(3))

		By("listing with MatchingLabels filters correctly")
		filtered, err := dummyStore.List(ctx, store.MatchingLabels{"app": "foo"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(2))
		for _, obj := range filtered {
			Expect(obj.GetLabels()["app"]).To(Equal("foo"))
		}

		By("listing with multiple label selectors")
		filtered, err = dummyStore.List(ctx, store.MatchingLabels{"app": "foo", "env": "prod"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(1))
		Expect(filtered[0].GetID()).To(Equal("labeled-a"))

		By("listing with HasLabels filters by key existence")
		filtered, err = dummyStore.List(ctx, store.HasLabels{"env"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(3))

		By("listing with HasLabels for a non-existent key")
		filtered, err = dummyStore.List(ctx, store.HasLabels{"missing"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(BeEmpty())

		By("combining MatchingLabels and HasLabels")
		filtered, err = dummyStore.List(ctx, store.MatchingLabels{"env": "prod"}, store.HasLabels{"app"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(2))
	})

	It("should filter objects by fields using MatchingFields", func(ctx SpecContext) {
		By("creating objects with different spec values")
		createDummy(ctx, dummyStoreField, "field-ssd-a", nil, "ssd")
		createDummy(ctx, dummyStoreField, "field-ssd-b", nil, "ssd")
		createDummy(ctx, dummyStoreField, "field-hdd", nil, "hdd")

		By("listing with MatchingFields filters by field value")
		filtered, err := dummyStoreField.List(ctx, store.MatchingFields{"spec": "ssd"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(2))
		for _, obj := range filtered {
			Expect(obj.Spec).To(Equal("ssd"))
		}

		By("listing with MatchingFields returns nothing for unmatched value")
		filtered, err = dummyStoreField.List(ctx, store.MatchingFields{"spec": "nvme"})
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(BeEmpty())
	})

	It("should return an error when List references an unindexed field", func(ctx SpecContext) {
		_, err := dummyStoreField.List(ctx, store.MatchingFields{"nonexistent": "value"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unindexed field"))
	})

	It("should apply label and field selectors as AND when both are set", func(ctx SpecContext) {
		createDummy(ctx, dummyStoreField, "combined-a", map[string]string{"env": "prod"}, "ssd")
		createDummy(ctx, dummyStoreField, "combined-b", map[string]string{"env": "prod"}, "hdd")
		createDummy(ctx, dummyStoreField, "combined-c", map[string]string{"env": "dev"}, "ssd")

		By("only the object matching both selectors is returned")
		filtered, err := dummyStoreField.List(ctx,
			store.MatchingLabels{"env": "prod"},
			store.MatchingFields{"spec": "ssd"},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(1))
		Expect(filtered[0].GetID()).To(Equal("combined-a"))
	})
})
