package cache

import (
	"awesomeProject/internal/model"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type Cache struct {
	client *redis.Client
	ttl    time.Duration
	logger *slog.Logger
	mem    map[string]model.Order
	mu     sync.RWMutex
}

func NewCache(logger *slog.Logger) (*Cache, error) {
	addr := getEnv("REDIS_ADDR", "localhost:6379")
	password := getEnv("REDIS_PASSWORD", "")
	db, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	c := &Cache{
		client: client,
		ttl:    10 * time.Minute,
		logger: logger,
		mem:    make(map[string]model.Order, 1024),
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		logger.Warn("redis unavailable, using in-memory cache only",
			slog.String("addr", addr), slog.Any("err", err))
		c.client = nil
		return c, nil
	}
	logger.Info("connected to redis", slog.String("addr", addr), slog.Int("db", db))
	return c, nil
}

func (c *Cache) Set(order model.Order) {
	c.mu.Lock()
	c.mem[order.Order_uid] = order
	c.mu.Unlock()
	ctx := context.Background()
	data, _ := json.Marshal(order)
	if c.client != nil {
		if err := c.client.Set(ctx, order.Order_uid, data, c.ttl).Err(); err != nil {
			c.logger.Error("redis set failed", slog.String("key", order.Order_uid), slog.Any("err", err))
		}
	}
}

func (c *Cache) Get(id string) (model.Order, bool) {
	c.mu.RLock()
	if o, ok := c.mem[id]; ok {
		c.mu.RUnlock()
		return o, true
	}
	c.mu.RUnlock()
	ctx := context.Background()
	if c.client == nil {
		return model.Order{}, false
	}
	val, err := c.client.Get(ctx, id).Result()
	if errors.Is(err, redis.Nil) {
		return model.Order{}, false
	}
	if err != nil {
		c.logger.Error("redis get failed", slog.String("key", id), slog.Any("err", err))
		return model.Order{}, false
	}
	var o model.Order
	if err := json.Unmarshal([]byte(val), &o); err != nil {
		c.logger.Error("unmarshal from redis failed", slog.Any("err", err))
		return model.Order{}, false
	}
	c.mu.Lock()
	c.mem[id] = o
	c.mu.Unlock()
	return o, true
}
func (c *Cache) BulkSet(list []model.Order) {
	for _, order := range list {
		c.Set(order)
	}
}
