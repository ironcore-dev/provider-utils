package claim_test

import (
	"github.com/ironcore-dev/ironcore/api/core/v1alpha1"
	"github.com/ironcore-dev/provider-utils/claimutils/claim"
	"github.com/ironcore-dev/provider-utils/claimutils/gpu"
	"github.com/ironcore-dev/provider-utils/claimutils/pci"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

type mockReader struct{}

func (*mockReader) Read() ([]pci.Address, error) {
	return nil, nil
}

var _ = Describe("Resource Claimer", func() {
	It("should claim composite resources", func(ctx SpecContext) {
		By("init plugin")

		resources, err := claim.NewResourceClaimer(
			gpu.NewGPUClaimPlugin(log.FromContext(ctx), "nvidia.com/gpu", &mockReader{}, []pci.Address{}),
		)
		Expect(err).NotTo(HaveOccurred())

		resourceClaim, err := resources.Claim(v1alpha1.ResourceList{
			"not_existing_plugin": resource.MustParse("1"),
		})
		Expect(err).To(MatchError(claim.ErrMissingPlugins))
		Expect(resourceClaim).To(BeNil())

		//resourceClaim, err := resources.Claim(v1alpha1.ResourceList{
		//	"nvidia.com/gpu": resource.MustParse("1"),
		//})
	})

})
