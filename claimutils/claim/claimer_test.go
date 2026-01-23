// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package claim_test

import (
	"context"

	"github.com/ironcore-dev/ironcore/api/core/v1alpha1"
	"github.com/ironcore-dev/provider-utils/claimutils/claim"
	"github.com/ironcore-dev/provider-utils/claimutils/gpu"
	"github.com/ironcore-dev/provider-utils/claimutils/pci"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

type mockReader struct {
	devices []pci.Address
	err     error
}

func (m *mockReader) Read() ([]pci.Address, error) {
	return m.devices, m.err
}

var _ = Describe("Resource Claimer", func() {
	It("should claim composite resources", func(ctx SpecContext) {
		By("init plugin")
		resourceClaimer, err := claim.NewResourceClaimer(
			gpu.NewGPUClaimPlugin(log.FromContext(ctx), "nvidia.com/gpu", &mockReader{
				devices: []pci.Address{
					{},
					{Function: 1},
				},
			}, nil),
		)
		Expect(err).NotTo(HaveOccurred())

		innerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// start the Runnable in the background
		errCh := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			errCh <- resourceClaimer.Start(innerCtx)
		}()

		By("failing if not not existing resource is claimed")
		resourceClaim, err := resourceClaimer.Claim(ctx, v1alpha1.ResourceList{
			"not_existing_plugin": resource.MustParse("1"),
		})
		Expect(err).To(MatchError(claim.ErrMissingPlugins))
		Expect(resourceClaim).To(BeNil())

		By("claiming correct resource")
		resourceClaim, err = resourceClaimer.Claim(ctx, v1alpha1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(resourceClaim).NotTo(BeNil())
		Expect(resourceClaim).To(HaveKey(v1alpha1.ResourceName("nvidia.com/gpu")))

		gpuClaim, ok := resourceClaim[v1alpha1.ResourceName("nvidia.com/gpu")].(gpu.Claim)
		Expect(ok).To(BeTrue())
		Expect(gpuClaim.PCIAddresses()).To(Not(BeNil()))

		By("releasing resource")
		Expect(resourceClaimer.Release(ctx, resourceClaim)).NotTo(HaveOccurred())

		By("claiming correct resource")
		resourceClaim, err = resourceClaimer.Claim(ctx, v1alpha1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("2"),
		})
		Expect(err).NotTo(HaveOccurred())

		By("claiming again resource")
		resourceClaim, err = resourceClaimer.Claim(ctx, v1alpha1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("2"),
		})
		Expect(err).Should(MatchError(claim.ErrInsufficientResources))

	})

})
