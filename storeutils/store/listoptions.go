// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"k8s.io/apimachinery/pkg/labels"
)

type ListOptions struct {
	LabelSelector labels.Selector
}

type ListOption interface {
	ApplyToList(*ListOptions)
}
