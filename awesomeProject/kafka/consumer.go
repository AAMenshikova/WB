package kafka

import (
	"awesomeProject/internal/model"
	"awesomeProject/internal/service"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
	svc    *service.Service
}

func NewConsumer(svc *service.Service, logger *slog.Logger) *Consumer {
	brokers := splitAndTrim(getEnv("KAFKA_BROKERS", "kafka:9092"))
	topic := getEnv("KAFKA_TOPIC", "orders")
	groupID := getEnv("KAFKA_GROUP_ID", "order-consumer-1")
	minBytes := int64(getEnvInt("KAFKA_MIN_BYTES", 1<<10))
	maxBytes := int64(getEnvInt("KAFKA_MAX_BYTES", 10<<20))
	startOffset := getEnv("KAFKA_START_OFFSET", "latest")
	cfg := kafka.ReaderConfig{
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupID,
		MinBytes:              int(minBytes),
		MaxBytes:              int(maxBytes),
		CommitInterval:        0,
		ReadBackoffMin:        100 * time.Millisecond,
		ReadBackoffMax:        2 * time.Second,
		WatchPartitionChanges: true,
		SessionTimeout:        10 * time.Second,
		HeartbeatInterval:     3 * time.Second,
		RebalanceTimeout:      10 * time.Second,
		QueueCapacity:         100,
	}
	switch strings.ToLower(startOffset) {
	case "earliest":
		cfg.StartOffset = kafka.FirstOffset
	default:
		cfg.StartOffset = kafka.LastOffset
	}
	reader := kafka.NewReader(cfg)
	logger.Info("kafka consumer configured",
		slog.String("brokers", strings.Join(brokers, ",")),
		slog.String("topic", topic),
		slog.String("group_id", groupID),
		slog.String("start_offset", startOffset),
	)
	return &Consumer{
		reader: reader,
		logger: logger,
		svc:    svc,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		_ = c.reader.Close()
	}()

	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("kafka consumer stopped", slog.Any("reason", ctx.Err()))
				return nil
			}
			c.logger.Error("kafka read message failed", slog.Any("err", err))
			continue
		}

		var order model.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			c.logger.Error("kafka message unmarshal failed",
				slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset),
				slog.Any("err", err))
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}
		if order.Order_uid == "" {
			c.logger.Error("kafka message without order_uid, skip",
				slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset))
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}

		if err := c.svc.UpsertOrder(ctx, order); err != nil {
			c.logger.Error("upsert order failed",
				slog.String("order_uid", order.Order_uid),
				slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset),
				slog.Any("err", err))
			continue
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			c.logger.Error("commit offset failed",
				slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset),
				slog.Any("err", err))
			continue
		}

		c.logger.Info("kafka message processed",
			slog.String("order_uid", order.Order_uid),
			slog.Int("partition", m.Partition),
			slog.Int64("offset", m.Offset),
		)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
