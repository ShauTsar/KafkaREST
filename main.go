package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	KafkaBroker = "localhost:9092" // Оставляем localhost:9092, т.к. сервис и Kafka на одном сервере
	Topic       = "obmenZupRegToUPR"
	// URL 1С УПР - временно отключаем, пока программист не настроит приемник
	// OneCURL       = "http://localhost:8081/onec_update"
	OneCURL       = ""                       // Пустой URL для отключения автоматической отправки
	LogFilePath   = "logs/kafka_service.log" // Путь к файлу логов
	RetryCount    = 3                        // Количество попыток отправки в 1С
	RetryDelay    = 5 * time.Second          // Задержка между попытками
	MaxBatchSize  = 10                       // Максимальное количество сообщений в батче
	ConsumerGroup = "1C-Consumers"           // Группа потребителей
	ListenAddress = ":8080"                  // Адрес для прослушивания HTTP
)

var (
	logger *log.Logger
)

// Структура для ответа
type Response struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Count   int         `json:"count,omitempty"`
}

// Инициализация логгера
func initLogger() error {
	// Создаем директорию для логов, если она не существует
	logDir := filepath.Dir(LogFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию для логов: %v", err)
	}

	// Открываем файл для логов (создаем, если не существует)
	logFile, err := os.OpenFile(LogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл логов: %v", err)
	}

	// Создаем мультирайтер для записи в файл и консоль
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(multiWriter, "", log.Ldate|log.Ltime|log.Lshortfile)
	return nil
}

// Producer (отправка в Kafka)
func produceMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondWithJSON(w, http.StatusMethodNotAllowed, Response{
			Status:  "error",
			Message: "Метод не поддерживается, используйте POST",
		})
		return
	}

	// Читаем тело запроса напрямую как байты
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Printf("Ошибка чтения тела запроса: %v", err)
		respondWithJSON(w, http.StatusBadRequest, Response{
			Status:  "error",
			Message: "Ошибка чтения данных",
		})
		return
	}

	// Проверяем, что это валидный JSON (опционально)
	if !json.Valid(body) {
		logger.Printf("Некорректный JSON: %s", string(body))
		respondWithJSON(w, http.StatusBadRequest, Response{
			Status:  "error",
			Message: "Некорректный JSON формат",
		})
		return
	}

	// Добавляем timestamp, парсим JSON как RawMessage
	var rawMsg json.RawMessage
	if err := json.Unmarshal(body, &rawMsg); err != nil {
		logger.Printf("Ошибка парсинга JSON: %v", err)
		respondWithJSON(w, http.StatusBadRequest, Response{
			Status:  "error",
			Message: "Ошибка парсинга JSON",
		})
		return
	}

	msg := struct {
		Data      json.RawMessage `json:"data"`
		Timestamp string          `json:"timestamp"`
	}{
		Data:      rawMsg,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	messageBytes, err := json.Marshal(msg)
	if err != nil {
		logger.Printf("Ошибка сериализации: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, Response{
			Status:  "error",
			Message: "Ошибка сериализации сообщения",
		})
		return
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(KafkaBroker),
		Topic:    Topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: messageBytes,
	})
	if err != nil {
		logger.Printf("Ошибка записи в Kafka: %v", err)
		respondWithJSON(w, http.StatusInternalServerError, Response{
			Status:  "error",
			Message: "Ошибка отправки сообщения в Kafka",
		})
		return
	}

	logger.Printf("✅ Сообщение отправлено в Kafka: %s", string(messageBytes))
	respondWithJSON(w, http.StatusOK, Response{
		Status:  "success",
		Message: "Сообщение успешно отправлено в Kafka",
	})
}

// Фоновый Consumer (автоматически отправляет в 1С)
// Временно отключен, чтобы не конфликтовать с ручным получением данных
func startConsumer(ctx context.Context) {
	// Если URL для 1С не задан, не запускаем автоматический Consumer
	if OneCURL == "" {
		logger.Println("Автоматический Consumer отключен (URL для 1С не настроен)")
		return
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{KafkaBroker},
		Topic:          Topic,
		GroupID:        ConsumerGroup + "-auto",
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})

	defer reader.Close()
	logger.Println("Kafka Consumer запущен, слушает сообщения...")

	for {
		select {
		case <-ctx.Done():
			logger.Println("Остановка фонового Consumer...")
			return
		default:
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				logger.Printf("Ошибка чтения сообщения: %v", err)
				time.Sleep(RetryDelay)
				continue
			}

			logger.Printf("Получено сообщение: %s", string(m.Value))
			success := sendTo1C(m.Value)

			// Только если успешно отправили в 1С, подтверждаем сообщение
			if success {
				if err := reader.CommitMessages(context.Background(), m); err != nil {
					logger.Printf("Ошибка подтверждения сообщения: %v", err)
				}
			} else {
				// Если не удалось отправить, ждем перед следующей попыткой
				time.Sleep(RetryDelay)
			}
		}
	}
}

// Ручной Consumer (по запросу GET)
func consumeMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		respondWithJSON(w, http.StatusMethodNotAllowed, Response{
			Status:  "error",
			Message: "Метод не поддерживается, используйте GET",
		})
		return
	}

	// Увеличиваем timeout для чтения сообщений
	readTimeout := 10 * time.Second

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{KafkaBroker},
		Topic:          Topic,
		GroupID:        ConsumerGroup + "-manual",
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})
	defer reader.Close()

	logger.Printf("Запрос на чтение сообщений из Kafka (timeout: %v)", readTimeout)

	var messages []json.RawMessage
	timeout := time.After(readTimeout)

	for len(messages) < MaxBatchSize {
		select {
		case <-timeout:
			if len(messages) == 0 {
				logger.Printf("Таймаут чтения из Kafka, сообщения не найдены")
				respondWithJSON(w, http.StatusOK, Response{
					Status:  "success",
					Message: "Нет новых данных",
					Data:    []json.RawMessage{},
					Count:   0,
				})
				return
			}
			logger.Printf("Таймаут чтения из Kafka, отправляем %d сообщений", len(messages))
			respondWithJSON(w, http.StatusOK, Response{
				Status: "success",
				Data:   messages,
				Count:  len(messages),
			})
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			m, err := reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				// Обработка ошибок как в оригинале
				continue
			}

			var msg struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(m.Value, &msg); err != nil {
				logger.Printf("Ошибка десериализации сообщения: %v", err)
				continue
			}

			logger.Printf("Прочитано сообщение из Kafka: %s", string(m.Value))
			messages = append(messages, msg.Data)

			if err := reader.CommitMessages(context.Background(), m); err != nil {
				logger.Printf("Ошибка подтверждения сообщения: %v", err)
			} else {
				logger.Printf("Сообщение подтверждено (commit) в Kafka")
			}
		}
	}

	logger.Printf("Достигнут MaxBatchSize (%d), отправляем сообщения", MaxBatchSize)
	respondWithJSON(w, http.StatusOK, Response{
		Status: "success",
		Data:   messages,
		Count:  len(messages),
	})
}

// Функция отправки данных в 1С
func sendTo1C(data []byte) bool {
	// Если URL не настроен, считаем что сообщение прочитано успешно
	if OneCURL == "" {
		logger.Printf("URL для 1С не настроен, пропускаем отправку")
		return true
	}

	client := &http.Client{Timeout: 10 * time.Second}
	var lastError error

	for i := 0; i < RetryCount; i++ {
		req, err := http.NewRequest("POST", OneCURL, bytes.NewBuffer(data)) // Передаём данные
		if err != nil {
			lastError = err
			logger.Printf("Попытка %d: Ошибка создания запроса: %v", i+1, err)
			time.Sleep(RetryDelay)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastError = err
			logger.Printf("Попытка %d: Ошибка отправки запроса: %v", i+1, err)
			time.Sleep(RetryDelay)
			continue
		}

		// Читаем ответ
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Логируем ответ от 1С, если он есть
		if readErr == nil && len(respBody) > 0 {
			logger.Printf("Ответ от 1С: %s", string(respBody))
		}

		if resp.StatusCode == http.StatusOK {
			logger.Printf("✅ Данные успешно отправлены в 1С (попытка %d): %s", i+1, string(data))
			return true
		}

		lastError = fmt.Errorf("1С вернула ошибку: %d", resp.StatusCode)
		logger.Printf("Попытка %d: %v", i+1, lastError)
		time.Sleep(RetryDelay)
	}

	logger.Printf("❌ Не удалось отправить данные в 1С после %d попыток: %v", RetryCount, lastError)
	return false
}

// Вспомогательная функция для отправки JSON-ответа
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		logger.Printf("Ошибка сериализации ответа: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"status":"error","message":"Ошибка формирования ответа"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

// Главная страница с информацией о сервисе
func homePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	info := map[string]string{
		"service":   "Сервис обмена данными между 1С ЗУП РЕГ и УПР через Kafka",
		"version":   "1.0",
		"endpoints": "POST /produce - отправка данных в Kafka, GET /consume - получение данных из Kafka",
		"status":    "работает",
		"time":      time.Now().Format(time.RFC3339),
	}

	respondWithJSON(w, http.StatusOK, info)
}

// Проверка статуса сервиса
func healthCheck(w http.ResponseWriter, r *http.Request) {
	status := map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	}
	respondWithJSON(w, http.StatusOK, status)
}

// Главная функция
func main() {
	// Инициализация логгера
	if err := initLogger(); err != nil {
		log.Fatalf("Ошибка инициализации логгера: %v", err)
	}

	logger.Println("Запуск сервиса обмена данными между 1С ЗУП РЕГ и УПР...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запуск фонового потребителя (если настроен URL для 1С)
	go startConsumer(ctx)

	// Настройка маршрутов
	http.HandleFunc("/", homePage)
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/produce", produceMessage)
	http.HandleFunc("/consume", consumeMessages)

	logger.Printf("REST API сервер запущен на %s...\n", ListenAddress)
	logger.Fatal(http.ListenAndServe(ListenAddress, nil))
}
