package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tangthinker/redis-delay-queue/delayqueue"
)

func main() {
	// 初始化Redis客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "10.66.66.1:6379",
		Password: "", // 如果没有密码，置空
		DB:       0,  // 默认DB
	})

	// 创建延迟队列实例
	queue := delayqueue.NewDelayQueue(redisClient)

	// 创建上下文
	ctx := context.Background()

	// 创建一个示例消息
	msg := &delayqueue.DelayMessage{
		ID:      "msg001",
		Topic:   "test_topic",
		Payload: json.RawMessage(`{"content": "这是一条测试消息"}`),
		Delay:   5 * time.Second, // 5秒后执行
	}

	// 发送消息到队列
	if err := queue.Push(ctx, msg); err != nil {
		log.Fatalf("Failed to push message: %v", err)
	}
	fmt.Printf("消息已发送，将在 %v 后执行\n", msg.Delay)

	// 等待并接收消息
	go func() {
		for {
			// 尝试获取消息
			msg, err := queue.Pop(ctx, "test_topic")
			if err != nil {
				log.Printf("Failed to pop message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			// 如果没有可用的消息，等待一秒后重试
			if msg == nil {
				time.Sleep(time.Second)
				continue
			}

			// 处理消息
			fmt.Printf("收到消息: ID=%s, Topic=%s, Payload=%s\n",
				msg.ID, msg.Topic, string(msg.Payload))
		}
	}()

	// 运行10秒后退出
	time.Sleep(10 * time.Second)
}
