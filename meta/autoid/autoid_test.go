// Copyright 2015 PingCAP, Inc.
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

package autoid_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/zmix999/goInception-plus/kv"
	"github.com/zmix999/goInception-plus/meta"
	"github.com/zmix999/goInception-plus/meta/autoid"
	"github.com/zmix999/goInception-plus/parser/model"
	"github.com/zmix999/goInception-plus/store/mockstore"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

// TestConcurrentAlloc is used for the test that
// multiple allocators allocate ID with the same table ID concurrently.
func TestConcurrentAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	autoid.SetStep(100)
	defer func() {
		autoid.SetStep(5000)
	}()

	dbID := int64(2)
	tblID := int64(100)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocIDs := func() {
		ctx := context.Background()
		alloc := autoid.NewAllocator(store, dbID, tblID, false, autoid.RowIDAllocType)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			_, id, err1 := alloc.Alloc(ctx, 1, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			mu.Lock()
			if _, ok := m[id]; ok {
				errCh <- fmt.Errorf("duplicate id:%v", id)
				mu.Unlock()
				break
			}
			m[id] = struct{}{}
			mu.Unlock()

			// test Alloc N
			N := rand.Uint64() % 100
			min, max, err1 := alloc.Alloc(ctx, N, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			for i := min + 1; i <= max; i++ {
				if _, ok := m[i]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", i)
					errFlag = true
					mu.Unlock()
					break
				}
				m[i] = struct{}{}
			}
			if errFlag {
				break
			}
			mu.Unlock()
		}
	}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocIDs()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	require.NoError(t, err)
}

// TestRollbackAlloc tests that when the allocation transaction commit failed,
// the local variable base and end doesn't change.
func TestRollbackAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	ctx := context.Background()
	injectConf := new(kv.InjectionConfig)
	injectConf.SetCommitError(errors.New("injected"))
	injectedStore := kv.NewInjectedStore(store, injectConf)
	alloc := autoid.NewAllocator(injectedStore, 1, 2, false, autoid.RowIDAllocType)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())

	err = alloc.Rebase(context.Background(), 100, true)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())
}

// TestNextStep tests generate next auto id step.
func TestNextStep(t *testing.T) {
	t.Parallel()
	nextStep := autoid.NextStep(2000000, 1*time.Nanosecond)
	require.Equal(t, int64(2000000), nextStep)
	nextStep = autoid.NextStep(678910, 10*time.Second)
	require.Equal(t, int64(678910), nextStep)
	nextStep = autoid.NextStep(50000, 10*time.Minute)
	require.Equal(t, int64(30000), nextStep)
}

// Fix a computation logic bug in allocator computation.
func TestAllocComputationIssue(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/zmix999/goInception-plus/meta/autoid/mockAutoIDCustomize", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/zmix999/goInception-plus/meta/autoid/mockAutoIDCustomize"))
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	unsignedAlloc1 := autoid.NewAllocator(store, 1, 1, true, autoid.RowIDAllocType)
	require.NotNil(t, unsignedAlloc1)
	signedAlloc1 := autoid.NewAllocator(store, 1, 1, false, autoid.RowIDAllocType)
	require.NotNil(t, signedAlloc1)
	signedAlloc2 := autoid.NewAllocator(store, 1, 2, false, autoid.RowIDAllocType)
	require.NotNil(t, signedAlloc2)

	// the next valid two value must be 13 & 16, batch size = 6.
	err = unsignedAlloc1.Rebase(context.Background(), 10, false)
	require.NoError(t, err)
	// the next valid two value must be 10 & 13, batch size = 6.
	err = signedAlloc2.Rebase(context.Background(), 7, false)
	require.NoError(t, err)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(unsignedAlloc1, 9, 9)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(signedAlloc1, 4, 6)

	ctx := context.Background()
	// Here will recompute the new allocator batch size base on new base = 10, which will get 6.
	min, max, err := unsignedAlloc1.Alloc(ctx, 2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, int64(10), min)
	require.Equal(t, int64(16), max)
	min, max, err = signedAlloc2.Alloc(ctx, 2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, int64(7), min)
	require.Equal(t, int64(13), max)
}
