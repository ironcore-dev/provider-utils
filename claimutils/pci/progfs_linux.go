package pci

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/prometheus/procfs/sysfs"
)

type Class uint32
type Vendor uint32

var (
	Class3DController Class = 0x030200

	VendorNvidia Vendor = 0x10de
)

type reader struct {
	log logr.Logger
	fs  sysfs.FS

	vendorFilter Vendor
	classFilter  Class
}

func NewReader(log logr.Logger, vendorFilter Vendor, classFilter Class) (*reader, error) {
	fs, err := sysfs.NewDefaultFS()
	if err != nil {
		return nil, fmt.Errorf("failed to open sysfs: %w", err)
	}

	return &reader{
		log:          log,
		fs:           fs,
		vendorFilter: vendorFilter,
		classFilter:  classFilter,
	}, nil

}

func (r *reader) Read() ([]Address, error) {
	devices, err := r.fs.PciDevices()
	if err != nil {
		return nil, fmt.Errorf("failed to read pci devices: %w", err)
	}

	var pciDevices []Address
	for _, device := range devices {
		r.log.V(3).Info("Found pci device", "device", device.Name())

		switch {
		case device.Class != uint32(r.classFilter):
			continue
		case device.Vendor != uint32(r.vendorFilter):
			continue
		default:
		}

		r.log.V(1).Info("Found matching pci device", device.Name())
		pciDevices = append(pciDevices, Address{
			Domain:   uint(device.Location.Segment),
			Bus:      uint(device.Location.Bus),
			Slot:     uint(device.Location.Device),
			Function: uint(device.Location.Function),
		})

	}

	return nil, nil
}
