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
	"sync"
	"time"

	"github.com/karlseguin/ccache"
)

// Manager contains functions to manage cache instances.
// It provides functions for creating new cache instances and list all of the names of existing cache instances
// Each cache instance has unique name and its own cache size and TTL configuration.
type Manager interface {
	GetCache(ctx context.Context, cacheName string, maxSize int64, ttl time.Duration) (CInterface, error)
	ListCacheNames() []string
}

// CInterface contains functions a cache instance need to provide
type CInterface interface {
	Delete(key string) bool

	Get(key string) interface{}
	Set(key string, val interface{})

	GetString(key string) string
	SetString(key string, val string)

	GetInt(key string) int
	SetInt(key string, val int)
}

type CCache struct {
	ctx      context.Context
	name     string
	cache    *ccache.Cache
	cacheTTL time.Duration
}

func (c *CCache) Set(key string, val interface{}) {
	c.cache.Set(key, val, c.cacheTTL)
}
func (c *CCache) Get(key string) interface{} {
	if cached := c.cache.Get(key); cached != nil {
		cached.Extend(c.cacheTTL)
		return cached.Value()
	}
	return nil
}

func (c *CCache) Delete(key string) bool {
	return c.cache.Delete(key)
}

func (c *CCache) SetString(key string, val string) {
	c.Set(key, val)
}

func (c *CCache) GetString(key string) string {
	val := c.Get(key)
	if val != nil {
		return c.Get(key).(string)
	}
	return ""
}

func (c *CCache) SetInt(key string, val int) {
	c.Set(key, val)
}

func (c *CCache) GetInt(key string) int {
	val := c.Get(key)
	if val != nil {
		return c.Get(key).(int)
	}
	return 0
}

type cacheManager struct {
	ctx              context.Context
	m                sync.Mutex
	configuredCaches map[string]CInterface // Cache manager maintain a list of named configured CCache, the names are unique
}

func (cm *cacheManager) GetCache(ctx context.Context, cacheName string, maxSize int64, ttl time.Duration) (CInterface, error) {
	cm.m.Lock()
	defer cm.m.Unlock()
	cache, exists := cm.configuredCaches[cacheName]
	if !exists {
		cache = &CCache{
			ctx:      ctx,
			name:     cacheName,
			cache:    ccache.New(ccache.Configure().MaxSize(maxSize)),
			cacheTTL: ttl,
		}
		cm.configuredCaches[cacheName] = cache
	}
	return cache, nil
}

func (cm *cacheManager) ListCacheNames() []string {
	keys := make([]string, 0, len(cm.configuredCaches))
	for k := range cm.configuredCaches {
		keys = append(keys, k)
	}
	return keys
}

func NewCacheManager(ctx context.Context, enabled bool) Manager {
	cm := &cacheManager{
		ctx:              ctx,
		configuredCaches: map[string]CInterface{},
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
	}
}
