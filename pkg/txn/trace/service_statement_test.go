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

package trace

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_AddStatementFilter(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "insert into trace_statement_filters (method, value) values") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.AddStatementFilter(statementContainsMethod, "abc")
	assert.Error(t, err)
}

func Test_ClearStatementFilters(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "truncate table trace_statement_filters") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.ClearStatementFilters()
	assert.Error(t, err)
}

func Test_RefreshStatementFilters(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "select method, value from trace_statement_filters") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.RefreshStatementFilters()
	assert.Error(t, err)
}
