package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"time"
)

const (
	KafkaBroker = "localhost:9092"
	Topic       = "obmenZupRegToUPR"
	OneCURL     = "http://localhost:8081/onec_update" // URL 1С УПР
)

// Producer (отправка в Kafka)
func produceMessage(w http.ResponseWriter, r *http.Request) {
	var msg map[string]interface{} // Гибкая структура

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	messageBytes, _ := json.Marshal(msg)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(KafkaBroker),
		Topic:    Topic,
		Balancer: &kafka.LeastBytes{},
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: messageBytes,
	})
	if err != nil {
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message sent to Kafka"))
}

// 50 MB 50e6
// Фоновый Consumer
func startConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    Topic,
		GroupID:  "1C-Consumers",
		MaxBytes: 10e6, // 10MB
	})

	defer reader.Close()

	log.Println("Kafka Consumer запущен, слушает сообщения...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Ошибка чтения сообщения: %v\n", err)
			continue
		}

		log.Printf("Получено сообщение: %s\n", string(m.Value))

		// Отправляем сообщение в 1С УПР
		go sendTo1C(m.Value)
	}
}

// Функция отправки данных в 1С
func sendTo1C(data []byte) {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("POST", OneCURL, nil)
	if err != nil {
		log.Printf("Ошибка создания запроса в 1С: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Body = http.NoBody

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Ошибка отправки в 1С: %v\n", err)
		return
	}
	defer resp.Body.Close()

	log.Printf("Ответ от 1С: %d\n", resp.StatusCode)
}

// Главная функция
func main() {
	// Запускаем Consumer в фоновом режиме
	go startConsumer()

	http.HandleFunc("/produce", produceMessage)

	fmt.Println("REST API сервер запущен на порту 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
