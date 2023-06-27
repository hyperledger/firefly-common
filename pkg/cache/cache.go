// Copyright © 2023 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/karlseguin/ccache"
)

// Manager contains functions to manage cache instances.
// It provides functions for creating new cache instances and list all of the names of existing cache instances
// Each cache instance has unique name and its own cache size and TTL configuration.
//
// Two modes of operation are available
//
//	Default behavior (when the normal Get() methods are used to access the cache):
//	    - Item is added to the cache
//	    - After the expiry time, it is automatically purged from the cache to release memory
//	       - ** THIS PART IS NOT IMPLEMENTED, AND A REAPER NEEDS TO BE ADDED **
//	    - While it's in memory, it is returned from the Get() methods
//	    - The TTL is extended on each access, so it's time-to-live since last access
//
//	Optional behavior (when calling code uses GetUnexpired() to access the cache):
//	    - The value is only returned if it has not expired yet (regardless of whether it has been reaped)
//	    - The TTL is NOT extended on each access
type Manager interface {
	// Get a cache by name, if a cache already exists with the same name, it will be returned as is without checking maxSize, ttl and enabled matches
	GetCache(ctx context.Context, namespace, name string, maxSize int64, ttl time.Duration, enabled bool) (CInterface, error)
	ListCacheNames(namespace string) []string
	ResetCaches(namespace string)
	IsEnabled() bool
}

// CInterface contains functions a cache instance need to provide
type CInterface interface {
	IsEnabled() bool
	Delete(key string) bool

	Get(key string) interface{}
	GetUnexpired(key string) interface{}
	Set(key string, val interface{})

	GetString(key string) string
	SetString(key string, val string)

	GetInt(key string) int
	SetInt(key string, val int)

	GetInt64(key string) int64
	SetInt64(key string, val int64)
}

type CCache struct {
	enabled   bool
	ctx       context.Context
	namespace string
	name      string
	cache     *ccache.Cache
	cacheTTL  time.Duration
}

func (c *CCache) Set(key string, val interface{}) {
	if c.enabled {
		c.cache.Set(key, val, c.cacheTTL)
	}
}
func (c *CCache) Get(key string) interface{} {
	if c.enabled {
		if cached := c.cache.Get(key); cached != nil {
			cached.Extend(c.cacheTTL)
			return cached.Value()
		}
	}
	return nil
}

// GetNotExpired retrieves from the cache, without extending the expiry time, and
// if the existing TTL has already popped (but the item has not yet been reaped)
// we will return nil.
func (c *CCache) GetUnexpired(key string) interface{} {
	if c.enabled {
		if cached := c.cache.Get(key); cached != nil && !cached.Expired() {
			return cached.Value()
		}
	}
	return nil
}

func (c *CCache) Delete(key string) bool {
	if c.enabled {
		return c.cache.Delete(key)
	}
	return false
}

func (c *CCache) SetString(key string, val string) {
	if c.enabled {
		c.Set(key, val)
	}
}

func (c *CCache) GetString(key string) string {
	if c.enabled {
		val := c.Get(key)
		if val != nil {
			return c.Get(key).(string)
		}
	}
	return ""
}

func (c *CCache) SetInt(key string, val int) {
	if c.enabled {
		c.Set(key, val)
	}
}

func (c *CCache) GetInt(key string) int {
	if c.enabled {
		val := c.Get(key)
		if val != nil {
			return c.Get(key).(int)
		}
	}
	return 0
}

func (c *CCache) SetInt64(key string, val int64) {
	if c.enabled {
		c.Set(key, val)
	}
}

func (c *CCache) GetInt64(key string) int64 {
	if c.enabled {
		val := c.Get(key)
		if val != nil {
			return c.Get(key).(int64)
		}
	}
	return 0
}

func (c *CCache) IsEnabled() bool {
	return c.enabled
}

type cacheManager struct {
	enabled          bool // global default, when set to true, each cache instance can have an override
	ctx              context.Context
	m                sync.Mutex
	configuredCaches map[string]*CCache // Cache manager maintain a list of named configured CCache, the names are unique
}

func (cm *cacheManager) GetCache(ctx context.Context, namespace, name string, maxSize int64, ttl time.Duration, enabled bool) (CInterface, error) {
	cm.m.Lock()
	defer cm.m.Unlock()
	fqName := fmt.Sprintf("%s:%s", namespace, name)
	cache, exists := cm.configuredCaches[fqName]
	enabledValue := enabled
	if !cm.IsEnabled() {
		// when cache manager is disabled, the enabled toggle doesn't make any difference
		enabledValue = false
	}
	if !exists {
		cache = &CCache{
			ctx:       ctx,
			namespace: namespace,
			name:      name,
			cache:     ccache.New(ccache.Configure().MaxSize(maxSize)),
			cacheTTL:  ttl,
			enabled:   enabledValue,
		}
		cm.configuredCaches[fqName] = cache
	}
	return cache, nil
}

func (cm *cacheManager) ListCacheNames(namespace string) []string {
	keys := make([]string, 0, len(cm.configuredCaches))
	for k, c := range cm.configuredCaches {
		if c.namespace == namespace {
			keys = append(keys, k)
		}
	}
	return keys
}

func (cm *cacheManager) ResetCaches(namespace string) {
	cm.m.Lock()
	defer cm.m.Unlock()
	for k, c := range cm.configuredCaches {
		if c.namespace == namespace {
			// Clear the cache to free the memory immediately
			c.cache.Clear()
			// Remove it from the map, so the next call will generate a new one
			delete(cm.configuredCaches, k)
		}
	}
}

func (cm *cacheManager) IsEnabled() bool {
	return cm.enabled
}

func NewCacheManager(ctx context.Context, enabled bool) Manager {
	cm := &cacheManager{
		ctx:              ctx,
		configuredCaches: map[string]*CCache{},
		enabled:          enabled,
	}
	return cm
}

// should only be used for testing purpose
func NewUmanagedCache(ctx context.Context, sizeLimit int64, ttl time.Duration) CInterface {
	return &CCache{
		ctx:      ctx,
		name:     "cache.unmanaged",
		cache:    ccache.New(ccache.Configure().MaxSize(sizeLimit)),
		cacheTTL: ttl,
		enabled:  true,
	}
}
