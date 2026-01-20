package claim

import (
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/ironcore-dev/ironcore/api/core/v1alpha1"
)

var (
	ErrMissingPlugins = errors.New("no plugin for resource")
	ErrReleaseClaim   = errors.New("failed to release claim")
)

type Claims map[v1alpha1.ResourceName]ResourceClaim

type Claimer interface {
	Claim(resources v1alpha1.ResourceList) (Claims, error)
	Release(claims Claims) error
}

func NewResourceClaimer(plugins ...Plugin) (*claimer, error) {
	c := claimer{
		plugins: map[string]Plugin{},
	}

	for _, plugin := range plugins {
		if err := plugin.Init(); err != nil {
			return nil, err
		}
		c.plugins[plugin.Name()] = plugin
	}
	return &c, nil
}

type claimer struct {
	log     logr.Logger
	plugins map[string]Plugin
}

func (c *claimer) checkPluginsForResources(resources v1alpha1.ResourceList) error {
	var missingPluginErrors []error
	for resourceName := range resources {
		if _, ok := c.plugins[string(resourceName)]; !ok {
			missingPluginErrors = append(missingPluginErrors, fmt.Errorf("plugin for resource %s not found", resourceName))
		}
	}
	if len(missingPluginErrors) > 0 {
		return errors.Join(missingPluginErrors...)
	}

	return nil
}

func (c *claimer) checkPluginsForClaims(claims Claims) error {
	var missingPluginErrors []error
	for resourceName := range claims {
		if _, ok := c.plugins[string(resourceName)]; !ok {
			missingPluginErrors = append(missingPluginErrors, fmt.Errorf("plugin for resource %s not found", resourceName))
		}
	}
	if len(missingPluginErrors) > 0 {
		return errors.Join(missingPluginErrors...)
	}

	return nil
}

func (c *claimer) Claim(resources v1alpha1.ResourceList) (Claims, error) {
	if err := c.checkPluginsForResources(resources); err != nil {
		return nil, errors.Join(ErrMissingPlugins, err)
	}

	var insufficientResourceErrors []error
	for resourceName := range resources {
		plugin := c.plugins[string(resourceName)]
		if !plugin.CanClaim(resources[resourceName]) {
			insufficientResourceErrors = append(insufficientResourceErrors, fmt.Errorf("insufficient resource for %s", resourceName))
		}
	}
	if len(insufficientResourceErrors) > 0 {
		return nil, errors.Join(ErrInsufficientResources, errors.Join(insufficientResourceErrors...))
	}

	claims := map[v1alpha1.ResourceName]ResourceClaim{}
	for resourceName := range resources {
		plugin := c.plugins[string(resourceName)]

		claim, claimErr := plugin.Claim(resources[resourceName])
		if claimErr != nil {
			if err := c.release(claims); err != nil {
				c.log.Error(errors.Join(ErrReleaseClaim, err), fmt.Sprintf("failed to release claim "))
			}
			return nil, claimErr
		}

		claims[resourceName] = claim
	}

	return nil, nil
}

func (c *claimer) release(claims Claims) error {
	var releaseErrors []error
	for resourceName := range claims {
		plugin := c.plugins[string(resourceName)]

		if err := plugin.Release(claims[resourceName]); err != nil {
			releaseErrors = append(releaseErrors, err)
		}
	}
	if len(releaseErrors) > 0 {
		return errors.Join(releaseErrors...)
	}

	return nil
}

func (c *claimer) Release(claims Claims) error {
	if err := c.checkPluginsForClaims(claims); err != nil {
		return errors.Join(ErrMissingPlugins, err)
	}

	if err := c.release(claims); err != nil {
		return errors.Join(ErrReleaseClaim, err)
	}

	return nil
}
