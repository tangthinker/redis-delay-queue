package delayqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DelayMessage 表示延迟队列中的消息
type DelayMessage struct {
	ID        string          `json:"id"`        // 消息唯一标识
	Topic     string          `json:"topic"`     // 消息主题
	Payload   json.RawMessage `json:"payload"`   // 消息内容
	Delay     time.Duration   `json:"delay"`     // 延迟时间
	Timestamp time.Time       `json:"timestamp"` // 消息创建时间
}

// DelayQueue 实现了基于Redis的延迟队列
type DelayQueue struct {
	redis *redis.Client
}

// NewDelayQueue 创建一个新的延迟队列实例
func NewDelayQueue(client *redis.Client) *DelayQueue {
	return &DelayQueue{redis: client}
}

// Push 将消息推送到延迟队列
func (q *DelayQueue) Push(ctx context.Context, msg *DelayMessage) error {
	if msg.ID == "" || msg.Topic == "" {
		return fmt.Errorf("message ID and topic are required")
	}

	// 设置消息创建时间
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// 序列化消息
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// 计算消息执行时间
	execTime := msg.Timestamp.Add(msg.Delay)

	// 将消息存储到Redis有序集合中
	key := fmt.Sprintf("delay_queue:%s", msg.Topic)
	member := redis.Z{
		Score:  float64(execTime.Unix()),
		Member: data,
	}

	_, err = q.redis.ZAdd(ctx, key, member).Result()
	return err
}

// Pop 从队列中获取到期的消息
func (q *DelayQueue) Pop(ctx context.Context, topic string) (*DelayMessage, error) {
	key := fmt.Sprintf("delay_queue:%s", topic)
	now := float64(time.Now().Unix())

	// 获取并删除最早的到期消息
	result, err := q.redis.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%f", now),
		Offset: 0,
		Count:  1,
	}).Result()

	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}

	// 解析消息
	var msg DelayMessage
	if err := json.Unmarshal([]byte(result[0]), &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %v", err)
	}

	// 从队列中删除消息
	if _, err := q.redis.ZRem(ctx, key, result[0]).Result(); err != nil {
		return nil, err
	}

	return &msg, nil
}
