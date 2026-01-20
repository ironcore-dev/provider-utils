package claim_test

import (
	"github.com/ironcore-dev/ironcore/api/core/v1alpha1"
	"github.com/ironcore-dev/provider-utils/claimutils/claim"
	"github.com/ironcore-dev/provider-utils/claimutils/gpu"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ = Describe("GPU Claimer", func() {
	It("should error if no resource left (not init)", func(ctx SpecContext) {
		By("init plugin")

		resourceManager, err := claim.NewResourceClaimer(
			gpu.NewGPUClaimPlugin(log.FromContext(ctx), "nvidia.com/gpu"),
		)
		Expect(err).NotTo(HaveOccurred())

		resourceClaim, err := resourceManager.Claim(v1alpha1.ResourceList{
			"not_existing_plugin": resource.MustParse("1"),
		})
		Expect(err).To(MatchError(claim.ErrMissingPlugins))
		Expect(resourceClaim).To(BeNil())
	})

})
