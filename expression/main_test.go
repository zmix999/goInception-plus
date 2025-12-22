// Copyright 2021 PingCAP, Inc.
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

package expression

import (
	"testing"
	"time"

	"gitee.com/zhoujin826/goInception-plus/util/mock"
	"github.com/stretchr/testify/require"
)

func createContext(t *testing.T) *mock.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	sc := ctx.GetSessionVars().StmtCtx
	sc.TruncateAsWarning = true
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("max_allowed_packet", "67108864"))
	ctx.GetSessionVars().PlanColumnID = 0
	return ctx
}
