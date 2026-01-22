package pci

import (
	"fmt"
)

type Address struct {
	Domain   uint
	Bus      uint
	Slot     uint
	Function uint
}

func (p Address) String() string {
	return fmt.Sprintf("%04x:%02x:%02x.%1x", p.Domain, p.Bus, p.Slot, p.Function)
}

type Reader interface {
	Read() ([]Address, error)
}
