package testutil

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartRedis runs Redis in a container and returns a client plus a function
// that removes the container. Call it from TestMain.
func StartRedis(ctx context.Context) (*goredis.Client, func(), error) {
	ctr, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return nil, nil, fmt.Errorf("start redis container: %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(ctr) }
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		stop()
		return nil, nil, err
	}
	opts, err := goredis.ParseURL(uri)
	if err != nil {
		stop()
		return nil, nil, err
	}
	rdb := goredis.NewClient(opts)
	return rdb, func() { _ = rdb.Close(); stop() }, nil
}
