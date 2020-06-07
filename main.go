package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	run()
}

func run() {
	brokers := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	data, err := readData()
	if err != nil {
		panic(err)
	}

	messages := make([]*sarama.ProducerMessage, 0)
	for _, v := range data {
		for _, topic := range v.Topics {
			for _, msg := range v.Messages {
				value, _ := json.Marshal(msg.Value)
				m := sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder("test"),
					Value: sarama.ByteEncoder(value),
				}

				messages = append(messages, &m)
			}
		}
	}

	total := len(messages)
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			select {
			case result := <-producer.Successes():
				fmt.Println("Sent message to topic:", result.Topic)
				count++
				if count == total {
					return
				}
			case err := <-producer.Errors():
				fmt.Println("Error while sending message,", err)
				return
			case <-done:
				fmt.Println("Terminating: via signal")
				return
			}
		}
	}()

	for _, v := range messages {
		producer.Input() <- v
	}

	wg.Wait()
	producer.Close()
	fmt.Println("<--------- DONE --------->")
}

// --------------- Message types ---------//
type (
	message struct {
		Header map[string]interface{} `json:"header,omitempty"`
		Key    interface{}            `json:"key,omitempty"`
		Value  interface{}            `json:"value,omitempty"`
	}

	data struct {
		Topics   []string   `json:"topics,omitempty"`
		Messages []*message `json:"messages,omitempty"`
	}
)

// --------------- Read data --------------//
func readData() ([]*data, error) {
	rs := make([]*data, 0)

	err := filepath.Walk("./data",
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				return err
			}

			allBytes, err := ioutil.ReadAll(file)
			if err != nil {
				return err
			}

			var d data
			err = json.Unmarshal(allBytes, &d)
			if err != nil {
				return err
			}

			rs = append(rs, &d)

			return nil
		})

	if err != nil {
		return nil, err
	}

	return rs, nil
}
