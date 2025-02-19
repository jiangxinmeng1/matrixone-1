// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fifocache

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func BenchmarkEnsureNBytesAndSet(b *testing.B) {
	cache := NewDataCache(
		fscache.ConstCapacity(1024),
		nil, nil, nil,
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.EnsureNBytes(1)
		}
	})
}

func TestShardCacheKey(t *testing.T) {
	var shards [numShards]int
	for offset := range int64(128) {
		for size := range int64(128) {
			sum := shardCacheKey(fscache.CacheKey{
				Path:   fmt.Sprintf("%v%v", offset, size),
				Offset: offset,
				Sz:     size,
			})
			shards[sum%numShards]++
		}
	}
	for _, n := range shards {
		if n == 0 {
			t.Fatal("not good")
		}
	}
}
