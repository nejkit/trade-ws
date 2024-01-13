package storage

import (
	"context"
	"time"

	redisLib "github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redisLib.Client
}

func NewRedisClient(host string) RedisClient {
	return RedisClient{cli: redisLib.NewClient(&redisLib.Options{
		Addr:     host,
		Password: "",
		DB:       0,
	})}
}

func (r *RedisClient) insertIntoList(ctx context.Context, key string, value []byte) error {
	txFunc := func(tx *redisLib.Tx) error {
		_, err := tx.LPush(ctx, key, value).Result()

		if err != nil {
			return err
		}

		ttl, err := tx.TTL(ctx, key).Result()

		if err != nil {
			return err
		}

		if ttl == -1 {
			_, err = tx.Expire(ctx, key, time.Minute*10).Result()
			if err != nil {
				return err
			}
		}

		return nil
	}

	if err := r.cli.Watch(ctx, txFunc, key); err != nil {
		return err
	}

	return nil
}

func (r *RedisClient) getFromList(ctx context.Context, key string) ([]byte, error) {
	value, err := r.cli.LPop(ctx, key).Result()

	if err != nil {
		return nil, err
	}

	return []byte(value), nil
}
