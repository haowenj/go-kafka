package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

var addr = "192.168.41.26:9092"
var topic = "topic-1"
var partition = 3

func init() {
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// 创建topic,重复执行没有影响
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partition,
			ReplicationFactor: 1,
		},
	}
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	//查询topic
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	// 遍历所有分区取topic
	for _, p := range partitions {
		if p.Topic != "__consumer_offsets" {
			fmt.Println(p.Topic, p.ID)
		}
	}
}

func main() {
	go consume()
	go consume()
	go consume()
	go consume2()
	go consumeFirst()
	// 创建一个writer 向topic-A发送消息
	w := &kafka.Writer{
		Addr: kafka.TCP(addr),
		//Topic:        topic, //msg里设置了就不能在writer里设置
		Balancer:     &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks: kafka.RequireAll,    // ack模式
		Async:        true,                // 异步
	}
	defer w.Close()
	var key, value string
	for {
		fmt.Println("input key and value....")
		fmt.Scan(&key, &value)

		if key == "exit" {
			break
		}

		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
				Topic: topic,
			},
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
				Topic: "my-first-topic",
			},
		)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func consume() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		GroupID:  "g1", // 指定消费者组id
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})
	ctx := context.Background()
	for true {
		// 获取消息
		m, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 显式提交
		if err := r.CommitMessages(ctx, m); err != nil {
			fmt.Println("failed to commit messages:", err)
		}
	}
}

// 模拟多个消费组消费同一个topic
func consume2() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		GroupID:  "g1-1", // 指定消费者组id
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})
	ctx := context.Background()
	for true {
		// 获取消息
		m, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 显式提交
		if err := r.CommitMessages(ctx, m); err != nil {
			fmt.Println("failed to commit messages:", err)
		}
	}
}

func consumeFirst() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		GroupID:  "g2", // 指定消费者组id
		Topic:    "my-first-topic",
		MaxBytes: 10e6, // 10MB
	})
	ctx := context.Background()
	for true {
		// 获取消息
		m, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 显式提交
		if err := r.CommitMessages(ctx, m); err != nil {
			fmt.Println("failed to commit messages:", err)
		}
	}
}
