// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package host_test

import (
	"os"
	"path/filepath"

	"github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
		_, err := dummyStore.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "labeled-a",
				Labels: map[string]string{"app": "foo", "env": "prod"},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStore.Delete, ctx, "labeled-a")

		_, err = dummyStore.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "labeled-b",
				Labels: map[string]string{"app": "bar", "env": "prod"},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStore.Delete, ctx, "labeled-b")

		_, err = dummyStore.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "labeled-c",
				Labels: map[string]string{"app": "foo", "env": "dev"},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStore.Delete, ctx, "labeled-c")

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
		_, err := dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{ID: "field-ssd-a"},
			Spec:     "ssd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "field-ssd-a")

		_, err = dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{ID: "field-ssd-b"},
			Spec:     "ssd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "field-ssd-b")

		_, err = dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{ID: "field-hdd"},
			Spec:     "hdd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "field-hdd")

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

	It("should apply label and field selectors as AND when both are set", func(ctx SpecContext) {
		_, err := dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "combined-a",
				Labels: map[string]string{"env": "prod"},
			},
			Spec: "ssd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "combined-a")

		_, err = dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "combined-b",
				Labels: map[string]string{"env": "prod"},
			},
			Spec: "hdd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "combined-b")

		_, err = dummyStoreField.Create(ctx, &Dummy{
			Metadata: api.Metadata{
				ID:     "combined-c",
				Labels: map[string]string{"env": "dev"},
			},
			Spec: "ssd",
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(dummyStoreField.Delete, ctx, "combined-c")

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
