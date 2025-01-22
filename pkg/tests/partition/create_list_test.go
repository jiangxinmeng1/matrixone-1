// Copyright 2021 - 2024 Matrix Origin
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

package partition

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDeleteListBased(t *testing.T) {
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c int comment 'abc') partition by list (c) (partition p1 values in (1) engine = innodb, partition p2 values in (2) engine = innodb)",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {
			require.Equal(t, fmt.Sprintf("values in (%d)", idx+1), p.Comment)
		},
	)
}

func TestCreateAndDeleteListColumnsBased(t *testing.T) {
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c DATETIME) partition by list columns (c) (partition p1 values in ('2024-01-21 00:00:00'), partition p2 values in ('2024-01-22 00:00:00'))",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {
			require.Equal(t, fmt.Sprintf("values in ('2024-01-2%d 00:00:00')", idx+1), p.Comment)
		},
	)
}
