// Copyright 2022 Matrix Origin
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

package cdc

import (
	"sync"
)

type memQ[T any] struct {
	sync.Mutex
	data []T
}

func (mq *memQ[T]) Push(v T) {
	mq.Lock()
	defer mq.Unlock()
	mq.data = append(mq.data, v)
}

func (mq *memQ[T]) Pop() {
	mq.Lock()
	defer mq.Unlock()
	mq.data = mq.data[1:]
}

func (mq *memQ[T]) Front() T {
	mq.Lock()
	defer mq.Unlock()
	return mq.data[0]
}

func (mq *memQ[T]) Back() T {
	mq.Lock()
	defer mq.Unlock()
	return mq.data[len(mq.data)-1]
}

func (mq *memQ[T]) Size() int {
	mq.Lock()
	defer mq.Unlock()
	return len(mq.data)
}

func (mq *memQ[T]) Empty() bool {
	mq.Lock()
	defer mq.Unlock()
	return len(mq.data) == 0
}
