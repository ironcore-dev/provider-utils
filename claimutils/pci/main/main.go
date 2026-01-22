// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
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
		log.Info("PCI device", "device", address.String())
	}
}
