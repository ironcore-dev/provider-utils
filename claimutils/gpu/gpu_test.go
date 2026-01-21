package gpu

import (
	"github.com/ironcore-dev/provider-utils/claimutils/claim"
	"github.com/ironcore-dev/provider-utils/claimutils/pci"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ = Describe("GPU Claimer", func() {
	It("should error if no resource left (not init)", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
		}

		By("claim resources")
		_, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).To(MatchError(claim.ErrInsufficientResources))
	})

	It("should error if no resource left", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusClaimed,
			},
		}

		By("claim resources")
		_, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).To(MatchError(claim.ErrInsufficientResources))
	})

	It("should claim device if enough are present", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusFree,
			},
		}

		By("claim resources")
		gpuClaim, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(gpuClaim).NotTo(BeNil())

		By("claim resources again and fail")
		secondGpuClaim, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).To(MatchError(claim.ErrInsufficientResources))
		Expect(secondGpuClaim).To(BeNil())
	})

	It("should claim multiple devices", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusFree,
				pci.Address{
					Function: 1,
				}: ClaimStatusFree,
			},
		}

		By("claim resources to much resources")
		gpuClaim, err := plugin.Claim(resource.MustParse("10"))
		Expect(err).To(MatchError(claim.ErrInsufficientResources))
		Expect(gpuClaim).To(BeNil())

		By("claim resources")
		_, err = plugin.Claim(resource.MustParse("2"))
		Expect(err).To(BeNil())

		By("claim resources when not sufficient")
		_, err = plugin.Claim(resource.MustParse("1"))
		Expect(err).To(MatchError(claim.ErrInsufficientResources))
	})

	It("should claim different devices", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusFree,
				pci.Address{
					Function: 1,
				}: ClaimStatusFree,
			},
		}

		By("claim resources")
		gpuClaim1, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).To(BeNil())

		ociAddress1, ok := gpuClaim1.(Claim)
		Expect(ok).To(BeTrue())
		Expect(ociAddress1.PCIAddresses()).To(HaveLen(1))

		By("claim resources again")
		gpuClaim2, err := plugin.Claim(resource.MustParse("1"))
		Expect(err).To(BeNil())

		ociAddress2, ok := gpuClaim2.(Claim)
		Expect(ok).To(BeTrue())
		Expect(ociAddress2.PCIAddresses()).To(HaveLen(1))

		By("ensure claims are not equal")
		Expect(ociAddress1.PCIAddresses()[0]).NotTo(Equal(ociAddress2.PCIAddresses()[0]))
	})

	It("should claim different devices", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusFree,
				pci.Address{
					Function: 1,
				}: ClaimStatusFree,
			},
		}

		By("claim resources")
		claim, err := plugin.Claim(resource.MustParse("0"))
		Expect(err).To(BeNil())

		gpuClaim, ok := claim.(Claim)
		Expect(ok).To(BeTrue())
		Expect(gpuClaim.PCIAddresses()).To(BeEmpty())

	})

	It("should release devices", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusClaimed,
				pci.Address{
					Function: 1,
				}: ClaimStatusClaimed,
			},
		}

		gpuClaim := NewGPUClaim([]pci.Address{
			{},
			{
				Function: 1,
			},
			{
				Function: 2,
			},
		})

		Expect(plugin.Release(gpuClaim)).To(Succeed())

		By("claim resources")
		_, err := plugin.Claim(resource.MustParse("2"))
		Expect(err).To(BeNil())
	})

	It("should fail on generic claim", func(ctx SpecContext) {
		By("init plugin")
		plugin := &gpuClaimPlugin{
			log: log.FromContext(ctx),
			devices: map[pci.Address]ClaimStatus{
				pci.Address{}: ClaimStatusClaimed,
				pci.Address{
					Function: 1,
				}: ClaimStatusClaimed,
			},
		}

		Expect(plugin.Release(nil)).To(MatchError(claim.ErrInvalidResourceClaim))
	})

})
