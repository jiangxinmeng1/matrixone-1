// Copyright 2021 Matrix Origin
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

package catalog

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type DBMVCCNode struct{}

func NewEmptyDBMVCCNode() *DBMVCCNode {
	return &DBMVCCNode{}
}

func (e *DBMVCCNode) CloneAll() BaseNode {
	node := &DBMVCCNode{}
	return node
}

func (e *DBMVCCNode) CloneData() BaseNode {
	return &DBMVCCNode{}
}

func (e *DBMVCCNode) String() string {
	return ""
}

// for create drop in one txn
func (e *DBMVCCNode) Update(vun BaseNode) {}

func (e *DBMVCCNode) WriteTo(w io.Writer) (n int64, err error) { return }

func (e *DBMVCCNode) ReadFrom(r io.Reader) (n int64, err error) { return }

type DBNode struct {
	acInfo    accessInfo
	name      string
	datType   string
	createSql string
}

func (node *DBNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	if node.name, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	if sn, err = node.acInfo.ReadFrom(r); err != nil {
		return
	}
	n += sn
	if node.createSql, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	if node.datType, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	return
}

func (node *DBNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if sn, err = common.WriteString(node.name, w); err != nil {
		return
	}
	n += sn
	if sn, err = node.acInfo.WriteTo(w); err != nil {
		return
	}
	n += sn
	if sn, err = common.WriteString(node.createSql, w); err != nil {
		return
	}
	n += sn
	if sn, err = common.WriteString(node.datType, w); err != nil {
		return
	}
	n += sn
	return
}
