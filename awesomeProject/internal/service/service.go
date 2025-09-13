package service

import (
	"awesomeProject/internal/model"
	"context"
	"errors"
	"log/slog"
)

type Repository interface {
	InsertOrder(ctx context.Context, order model.Order) error
	GetOrderById(ctx context.Context, id string) (model.Order, error)
	LoadAll(ctx context.Context) ([]model.Order, error)
}

type Cache interface {
	Get(id string) (model.Order, bool)
	Set(o model.Order)
	BulkSet(list []model.Order)
}

type Service struct {
	repo   Repository
	cache  Cache
	logger *slog.Logger
}

func NewService(repo Repository, cache Cache, logger *slog.Logger) *Service {
	return &Service{repo, cache, logger}
}

func (s *Service) Warmup(ctx context.Context) error {
	orders, err := s.repo.LoadAll(ctx)
	if err != nil {
		s.logger.Error("warmup: load all failed", slog.Any("err", err))
		return err
	}
	if len(orders) > 0 {
		s.cache.BulkSet(orders)
	}
	s.logger.Info("cache warmup completed", slog.Int("orders", len(orders)))
	return nil
}

func (s *Service) GetOrderByID(ctx context.Context, id string) (model.Order, error) {
	if id == "" {
		return model.Order{}, errors.New("empty id")
	}
	if order, ok := s.cache.Get(id); ok {
		s.logger.Info("get order", slog.String("id", id), slog.Bool("cache_hit", true))
		return order, nil
	}
	order, err := s.repo.GetOrderById(ctx, id)
	if err != nil {
		s.logger.Error("get order by id failed", slog.Any("err", err))
		return model.Order{}, err
	}
	s.cache.Set(order)
	s.logger.Info("get order", slog.String("id", id), slog.Bool("cache_hit", false))
	return order, nil
}
func (s *Service) UpsertOrder(ctx context.Context, order model.Order) error {
	if order.Order_uid == "" {
		return errors.New("order_uid is empty")
	}
	if err := s.repo.InsertOrder(ctx, order); err != nil {
		return err
	}
	s.cache.Set(order)
	return nil
}

func (s *Service) UpsertMany(ctx context.Context, list []model.Order) error {
	for _, order := range list {
		if err := s.UpsertOrder(ctx, order); err != nil {
			return err
		}
	}
	return nil
}
