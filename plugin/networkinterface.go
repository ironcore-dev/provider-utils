// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"context"
	"net/netip"
)

type NetworkInterfaceState string

const (
	NetworkInterfaceStatePending NetworkInterfaceState = "Pending"
	NetworkInterfaceStateReady   NetworkInterfaceState = "Ready"
	NetworkInterfaceStateError   NetworkInterfaceState = "Error"
)

// NetworkInterface plugin definition to be implemented by the plugin and called
// from the provider. If the plugin requires initial setup it should offer a
// corresponding function for the provider to call.
type NetworkInterface interface {
	Name() string

	Apply(ctx context.Context, spec *NetworkInterfaceSpec, machineID string) (*NetworkInterfaceStatus, error)
	Delete(ctx context.Context, computeNicName string, machineID string) error
}

type NetworkInterfaceMetadata struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

type NetworkInterfaceSpec struct {
	Metadata  *NetworkInterfaceMetadata `json:"metadata"`
	Name      string                    `json:"name"`
	NetworkId string                    `json:"networkId"`
	Hostname  string                    `json:"hostname"`
	Ips       []string                  `json:"ips"`
}

// NetworkInterfaceStatus is returned by a plugin after successfully applying a
// network interface. It contains the effective configuration of the interface
// and the device type that the hypervisor can consume. A plugin must exactly
// one non-nil device type.
type NetworkInterfaceStatus struct {
	Handle string                `json:"handle"`
	State  NetworkInterfaceState `json:"state"`

	Prefixes  netip.Prefix `json:"prefixes"`
	Ips       []netip.Addr `json:"ips"`
	VirtualIp netip.Addr   `json:"virtual-ip"`

	PCI    *PCI
	TAP    *TAP
	Netdev *Netdev
}

// PCI address on the host to be passed through to the guest as a network
// interface.
type PCI struct {
	// BDF is the (Domain) / Bus / Device / Function address of the PCI
	// function in the format `(domain:)bus:device:function`.
	BDF string
}

// TAP device of which the hypervisor should take control of the fd-end to
// connect those to the net driver in the guest.
type TAP struct {
	Name string
}

// Netdev to which the guest should be connected, e.g. via a macvlan TAP. The
// netdev itself can be of any type.
type Netdev struct {
	Name string
}
