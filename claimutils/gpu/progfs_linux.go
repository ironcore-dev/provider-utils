package gpu

func (g *gpuClaimPlugin) Init() error {
	return g.init(g.devices)
}

func DiscoverStuff(used ClaimStatus) InitFunc
