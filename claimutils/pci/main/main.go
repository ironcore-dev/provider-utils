package main

import (
	"fmt"
	"github.com/ironcore-dev/provider-utils/claimutils/pci"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {

	log := zap.New(zap.UseDevMode(true))

	reader, err := pci.NewReader(log, pci.VendorNvidia, pci.Class3DController)
	if err != nil {
		log.Error(err, "Failed to create reader")
		return
	}

	addresses, err := reader.Read()
	if err != nil {
		log.Error(err, "Failed to create reader")
		return
	}

	for _, address := range addresses {
		log.Info(fmt.Sprintf("PCI device: %v", address.String()))
	}
}
