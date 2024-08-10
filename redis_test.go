package gredis

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

var rdb *RedDB

func init() {
	conf := &Config{
		PoolSize: 10,
		Hosts: []string{
			"10.240.10.154:8001",
			"10.240.10.154:8002",
			"10.240.10.154:8003",
		},
	}
	rdb = NewRedisDB(conf)
}

func TestPing(t *testing.T) {
	ping := rdb.client.Ping(context.Background())
	assert.Equal(t, "PONG", ping.Val())
}

func TestGet(t *testing.T) {
	get, err := rdb.Get("test")
	// 断言 err 为 nil
	assert.NoError(t, err, "Expected no error but got one")

	expectedValue := "test"
	assert.Equal(t, expectedValue, get, "Value should be equal to the expected value")

	// 断言 get 不为空
}
