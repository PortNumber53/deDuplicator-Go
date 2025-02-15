package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	semver "github.com/hashicorp/go-version"
	amqp "github.com/rabbitmq/amqp091-go"
)

// VersionUpdate represents a message indicating a new version deployment
type VersionUpdate struct {
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

// RabbitMQ holds the connection and channel information
type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	version string // Current version of the running instance
}

// NewRabbitMQ creates a new RabbitMQ connection using environment variables
func NewRabbitMQ(currentVersion string) (*RabbitMQ, error) {
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	user := os.Getenv("RABBITMQ_USER")
	pass := os.Getenv("RABBITMQ_PASSWORD")
	vhost := os.Getenv("RABBITMQ_VHOST")

	// Validate required environment variables
	if host == "" || port == "" || user == "" || pass == "" {
		return nil, fmt.Errorf("missing required RabbitMQ environment variables")
	}

	// If vhost doesn't start with '/', add it
	if vhost != "" && vhost[0] != '/' {
		vhost = "/" + vhost
	}

	// Build connection URL
	url := fmt.Sprintf("amqp://%s:%s@%s:%s%s",
		user, pass, host, port, vhost)

	log.Printf("Connecting to RabbitMQ at %s:%s%s...", host, port, vhost)

	// Connect to RabbitMQ with retry
	var conn *amqp.Connection
	var err error

	// Try to connect up to 3 times
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/3): %v", i+1, err)
		if i < 2 { // Don't sleep on the last attempt
			time.Sleep(2 * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after 3 attempts: %v", err)
	}

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	queueName := os.Getenv("RABBITMQ_QUEUE")
	if queueName == "" {
		queueName = "dedup_backup" // Default queue name
	}

	// Declare the queue
	q, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	log.Printf("Successfully connected to RabbitMQ and declared queue: %s", queueName)

	return &RabbitMQ{
		conn:    conn,
		channel: ch,
		queue:   q,
		version: currentVersion,
	}, nil
}

// Close closes the RabbitMQ connection and channel
func (r *RabbitMQ) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

// ListenForUpdates starts listening for version update messages
// It returns a channel that will be closed when a newer version update is received
func (r *RabbitMQ) ListenForUpdates(ctx context.Context) chan struct{} {
	shutdown := make(chan struct{})

	msgs, err := r.channel.Consume(
		r.queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
		close(shutdown)
		return shutdown
	}

	go func() {
		currentVer, err := semver.NewVersion(r.version)
		if err != nil {
			log.Printf("Error parsing current version %s: %v", r.version, err)
			close(shutdown)
			return
		}

		for {
			select {
			case <-ctx.Done():
				close(shutdown)
				return
			case msg, ok := <-msgs:
				if !ok {
					close(shutdown)
					return
				}

				var update VersionUpdate
				if err := json.Unmarshal(msg.Body, &update); err != nil {
					log.Printf("Error decoding version update message: %v", err)
					continue
				}

				newVer, err := semver.NewVersion(update.Version)
				if err != nil {
					log.Printf("Error parsing new version %s: %v", update.Version, err)
					continue
				}

				log.Printf("Received version update notification: version %s at %s",
					update.Version, update.Timestamp)

				if newVer.GreaterThan(currentVer) {
					log.Printf("New version %s is newer than current version %s, initiating shutdown",
						update.Version, r.version)
					close(shutdown)
					return
				} else {
					log.Printf("Ignoring version update as current version %s is not older than received version %s",
						r.version, update.Version)
				}
			}
		}
	}()

	return shutdown
}

// PublishVersionUpdate sends a version update message to the queue
func (r *RabbitMQ) PublishVersionUpdate(ctx context.Context, version string) error {
	// Validate that the version string is a valid semantic version
	_, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("invalid version format %s: %v", version, err)
	}

	update := VersionUpdate{
		Version:   version,
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal version update: %v", err)
	}

	err = r.channel.PublishWithContext(ctx,
		"",           // exchange
		r.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish version update: %v", err)
	}

	log.Printf("Published version update: %s", version)
	return nil
}
