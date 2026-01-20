package gpu

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/ironcore-dev/provider-utils/claimutils/claim"
	"k8s.io/apimachinery/pkg/api/resource"
)

type Claim interface {
	claim.ResourceClaim
	PCIAddresses() []PCIAddress
}

func NewGPUClaim(addresses []PCIAddress) Claim {
	return &gpuClaim{
		devices: addresses,
	}
}

type gpuClaim struct {
	devices []PCIAddress
}

func (c gpuClaim) PCIAddresses() []PCIAddress {
	return c.devices
}

type PCIAddress struct {
	Domain   uint
	Bus      uint
	Slot     uint
	Function uint
}

func (p PCIAddress) String() string {
	return fmt.Sprintf("%04x:%02x:%02x.%1x", p.Domain, p.Bus, p.Slot, p.Function)
}

type ClaimStatus bool

const (
	ClaimStatusFree    ClaimStatus = true
	ClaimStatusClaimed ClaimStatus = false
)

func NewGPUClaimPlugin(log logr.Logger, name string) claim.Plugin {
	return &gpuClaimPlugin{
		name: name,
		log:  log,
	}
}

type gpuClaimPlugin struct {
	name    string
	log     logr.Logger
	devices map[PCIAddress]ClaimStatus
}

func (g gpuClaimPlugin) canClaim(quantity resource.Quantity) bool {
	requested := quantity.Value()

	var free int64
	for _, claimed := range g.devices {
		if claimed == ClaimStatusFree {
			free++
		}
	}
	g.log.V(2).Info("Try to claim devices ", "free", free, "requested", requested)

	return free >= requested
}

func (g gpuClaimPlugin) CanClaim(quantity resource.Quantity) bool {
	return g.canClaim(quantity)
}

func (g gpuClaimPlugin) Claim(quantity resource.Quantity) (claim.ResourceClaim, error) {
	if !g.canClaim(quantity) {
		return nil, claim.ErrInsufficientResources
	}

	requested := quantity.Value()

	gClaim := &gpuClaim{}
	for device, claimed := range g.devices {
		if int64(len(gClaim.devices)) == requested {
			break
		}

		if claimed == ClaimStatusFree {
			g.devices[device] = ClaimStatusClaimed
			gClaim.devices = append(gClaim.devices, device)
		}
	}

	g.log.V(2).Info("Claimed devices", "devices", gClaim.devices)

	return gClaim, nil
}

func (g gpuClaimPlugin) Release(resourceClaim claim.ResourceClaim) error {
	gpu, ok := resourceClaim.(Claim)
	if !ok {
		return claim.ErrInvalidResourceClaim
	}

	pciAddresses := gpu.PCIAddresses()
	for _, pciAddress := range pciAddresses {
		if _, existing := g.devices[pciAddress]; !existing {
			g.log.V(2).Info("Device not managed by this plugin", "pciAddress", pciAddress)
			continue
		}

		g.log.V(3).Info("Unclaimed device", "pciAddress", pciAddress)
		g.devices[pciAddress] = ClaimStatusFree
	}

	return nil
}

func (g gpuClaimPlugin) Name() string {
	return g.name
}
