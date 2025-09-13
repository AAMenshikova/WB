package test

import (
	"awesomeProject/internal/model"
	"awesomeProject/internal/service"
	"context"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"
)

type mockRepo struct {
	insertFn  func(ctx context.Context, o model.Order) error
	getFn     func(ctx context.Context, id string) (model.Order, error)
	loadAllFn func(ctx context.Context) ([]model.Order, error)
}

func (m *mockRepo) InsertOrder(ctx context.Context, o model.Order) error {
	if m.insertFn != nil {
		return m.insertFn(ctx, o)
	}
	return nil
}
func (m *mockRepo) GetOrderById(ctx context.Context, id string) (model.Order, error) {
	if m.getFn != nil {
		return m.getFn(ctx, id)
	}
	return model.Order{}, nil
}
func (m *mockRepo) LoadAll(ctx context.Context) ([]model.Order, error) {
	if m.loadAllFn != nil {
		return m.loadAllFn(ctx)
	}
	return nil, nil
}

type mockCache struct {
	mem       map[string]model.Order
	setCount  int
	bulkCount int
}

func newMockCache() *mockCache { return &mockCache{mem: map[string]model.Order{}} }

func (c *mockCache) Get(id string) (model.Order, bool) {
	v, ok := c.mem[id]
	return v, ok
}
func (c *mockCache) Set(o model.Order) {
	c.setCount++
	c.mem[o.Order_uid] = o
}
func (c *mockCache) BulkSet(list []model.Order) {
	c.bulkCount += len(list)
	for _, o := range list {
		c.mem[o.Order_uid] = o
	}
}

func TestService_GetOrderByID_CacheHit(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cache := newMockCache()
	exp := model.Order{Order_uid: "id1", Track_number: "TN1"}
	cache.mem["id1"] = exp

	repo := &mockRepo{
		getFn: func(ctx context.Context, id string) (model.Order, error) {
			t.Fatalf("repo.GetOrderById не должен вызываться при cache hit")
			return model.Order{}, nil
		},
	}
	svc := service.NewService(repo, cache, logger)
	got, err := svc.GetOrderByID(context.Background(), "id1")
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("want %+v, got %+v", exp, got)
	}
	if cache.setCount != 0 {
		t.Fatalf("cache.Set не должен вызываться при cache hit")
	}
}

func TestService_GetOrderByID_CacheMissLoadsFromRepo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cache := newMockCache()
	exp := model.Order{Order_uid: "id2", Track_number: "TN2"}

	repo := &mockRepo{
		getFn: func(ctx context.Context, id string) (model.Order, error) {
			if id != "id2" {
				t.Fatalf("ожидали запрос id2, получили %s", id)
			}
			return exp, nil
		},
	}
	svc := service.NewService(repo, cache, logger)
	got, err := svc.GetOrderByID(context.Background(), "id2")
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("want %+v, got %+v", exp, got)
	}
	if cache.setCount != 1 {
		t.Fatalf("cache.Set должен быть вызван один раз (после miss)")
	}
	if cached, ok := cache.mem["id2"]; !ok || !reflect.DeepEqual(cached, exp) {
		t.Fatalf("значение должно быть положено в кэш")
	}
}

func TestService_Warmup_FillsCache(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cache := newMockCache()
	src := []model.Order{
		{Order_uid: "A"}, {Order_uid: "B"},
	}
	repo := &mockRepo{
		loadAllFn: func(ctx context.Context) ([]model.Order, error) {
			return src, nil
		},
	}
	svc := service.NewService(repo, cache, logger)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.Warmup(ctx); err != nil {
		t.Fatalf("неожиданная ошибка Warmup: %v", err)
	}
	if cache.bulkCount != len(src) {
		t.Fatalf("ожидали BulkSet по каждому заказу: %d, получили %d", len(src), cache.bulkCount)
	}
	for _, o := range src {
		if _, ok := cache.mem[o.Order_uid]; !ok {
			t.Fatalf("заказ %s должен оказаться в кэше", o.Order_uid)
		}
	}
}
