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

	messageBytes, err := json.Marshal(msg)
	if err != nil {
		http.Error(w, "Failed to marshal message", http.StatusInternalServerError)
		log.Printf("Ошибка сериализации: %v", err)
		return
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(KafkaBroker),
		Topic:    Topic,
		Balancer: &kafka.LeastBytes{},
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: messageBytes,
	})
	if err != nil {
		log.Printf("Ошибка записи в Kafka: %v", err)
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message sent to Kafka"))
}

// Consumer для ручного запроса сообщений
func consumeMessage(w http.ResponseWriter, r *http.Request) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    Topic,
		MaxBytes: 10e6, // 10MB
		// Без GroupID, чтобы читать напрямую, не привязываясь к группе
	})

	defer reader.Close()

	// Устанавливаем таймаут для чтения, чтобы не зависать
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		if err == context.DeadlineExceeded {
			http.Error(w, "No messages available within timeout", http.StatusNotFound)
			return
		}
		log.Printf("Ошибка чтения сообщения: %v", err)
		http.Error(w, "Failed to read message from Kafka", http.StatusInternalServerError)
		return
	}

	// Возвращаем сообщение в формате JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"offset": msg.Offset,
		"key":    string(msg.Key),
		"value":  string(msg.Value),
		"time":   msg.Time,
	})
}

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
			time.Sleep(5 * time.Second) // Ждем перед повторной попыткой
			continue
		}
		log.Printf("Получено сообщение: %s\n", string(m.Value))
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
	// Запускаем фоновый Consumer
	go startConsumer()

	// Регистрируем обработчики REST API
	http.HandleFunc("/produce", produceMessage)
	http.HandleFunc("/consume", consumeMessage)

	fmt.Println("REST API сервер запущен на порту 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
