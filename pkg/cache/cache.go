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

	// Get a cached value by key name
	//    - when extendTTL set to true, TTL of an item will be extended as part of this get call.
	//       This was the default behavior before the extendTTL parameter was introduced
	//    - when extendTTL set to false, the TTL of an item will not be extended.
	Get(key string, extendTTL bool) interface{}
	Set(key string, val interface{})

	// Get a cached string value by key name
	//    - when extendTTL set to true, TTL of an item will be extended as part of this get call.
	//       This was the default behavior before the extendTTL parameter was introduced
	//    - when extendTTL set to false, the TTL of an item will not be extended.
	GetString(key string, extendTTL bool) string
	SetString(key string, val string)

	// Get a cached int32 value by key name
	//    - when extendTTL set to true, TTL of an item will be extended as part of this get call.
	//       This was the default behavior before the extendTTL parameter was introduced
	//    - when extendTTL set to false, the TTL of an item will not be extended.
	GetInt(key string, extendTTL bool) int
	SetInt(key string, val int)

	// Get a cached int64 value by key name
	//    - when extendTTL set to true, TTL of an item will be extended as part of this get call.
	//       This was the default behavior before the extendTTL parameter was introduced
	//    - when extendTTL set to false, the TTL of an item will not be extended.
	GetInt64(key string, extendTTL bool) int64
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
func (c *CCache) Get(key string, extendTTL bool) interface{} {
	if c.enabled {
		if cached := c.cache.Get(key); cached != nil {
			if cached.Expired() {
				// TODO: need to implement a better reaper logic to remove other expired keys that weren't actively accessed using Get method.
				// Without the reaper, our cache will use up to the configured cache size.
				c.cache.Delete(key)
				return nil
			}
			if extendTTL {
				cached.Extend(c.cacheTTL)
			}
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

func (c *CCache) GetString(key string, extendTTL bool) string {
	if c.enabled {
		val := c.Get(key, extendTTL)
		if val != nil {
			return val.(string)
		}
	}
	return ""
}

func (c *CCache) SetInt(key string, val int) {
	if c.enabled {
		c.Set(key, val)
	}
}

func (c *CCache) GetInt(key string, extendTTL bool) int {
	if c.enabled {
		val := c.Get(key, extendTTL)
		if val != nil {
			return val.(int)
		}
	}
	return 0
}

func (c *CCache) SetInt64(key string, val int64) {
	if c.enabled {
		c.Set(key, val)
	}
}

func (c *CCache) GetInt64(key string, extendTTL bool) int64 {
	if c.enabled {
		val := c.Get(key, extendTTL)
		if val != nil {
			return val.(int64)
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
