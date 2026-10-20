package plugin

import (
	"context"
)

type NetworkInterfaceState string

const (
	NetworkInterfaceStatePending  NetworkInterfaceState = "Pending"
	NetworkInterfaceStateReady    NetworkInterfaceState = "Ready"
	NetworkInterfaceStateAttached NetworkInterfaceState = "Attached"
	NetworkInterfaceStateError    NetworkInterfaceState = "Error"
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
	Ips       []string                  `json:"ips"`
}

type NetworkInterfaceStatus struct {
	Name   string                `json:"name"`
	Handle string                `json:"handle"`
	State  NetworkInterfaceState `json:"state"`

	PCI    *PCI
	TAP    *TAP
	Netdev *Netdev
}

// PCI address on the host to be passed through to the guest as a network
// interface.
type PCI struct {
	Domain   uint
	Bus      uint
	Slot     uint
	Function uint
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
