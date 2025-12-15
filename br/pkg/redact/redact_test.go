// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package redact_test

import (
	"testing"

	"gitee.com/zhoujin826/goInception-plus/br/pkg/redact"
	. "github.com/pingcap/check"
)

type testRedactSuite struct{}

func (s *testRedactSuite) SetUpSuite(c *C)    {}
func (s *testRedactSuite) TearDownSuite(c *C) {}

var _ = Suite(&testRedactSuite{})

func TestT(t *testing.T) {}

func (s *testRedactSuite) TestRedact(c *C) {
	redacted, secret := "?", "secret"

	redact.InitRedact(false)
	c.Assert(redact.String(secret), Equals, secret)
	c.Assert(redact.Key([]byte(secret)), Equals, secret)

	redact.InitRedact(true)
	c.Assert(redact.String(secret), Equals, redacted)
	c.Assert(redact.Key([]byte(secret)), Equals, redacted)
}
