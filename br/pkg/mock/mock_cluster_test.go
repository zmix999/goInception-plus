// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package mock_test

import (
	"testing"

	"gitee.com/zhoujin826/goInception-plus/br/pkg/mock"
	"gitee.com/zhoujin826/goInception-plus/util/testleak"
	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	mock *mock.Cluster
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testClusterSuite) TestSmoke(c *C) {
	c.Assert(s.mock.Start(), IsNil)
	s.mock.Stop()
}
