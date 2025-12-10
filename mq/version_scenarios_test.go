package mq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	semver "github.com/hashicorp/go-version"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ackRecorder struct {
	acks []uint64
}

func (a *ackRecorder) Ack(tag uint64, _ bool) error {
	a.acks = append(a.acks, tag)
	return nil
}
func (a *ackRecorder) Nack(uint64, bool, bool) error { return nil }
func (a *ackRecorder) Reject(uint64, bool) error     { return nil }

func TestPublishVersionUpdateRejectsInvalidSemver(t *testing.T) {
	rabbit := &RabbitMQ{}
	if err := rabbit.PublishVersionUpdate(context.Background(), "not-a-semver"); err == nil {
		t.Fatalf("expected invalid semver to fail")
	}
}

func TestListenerLogicAcksAndShutsDownOnNewerVersion(t *testing.T) {
	current := "1.3.5"
	currentVer, err := semver.NewVersion(current)
	if err != nil {
		t.Fatalf("parse current version: %v", err)
	}

	msgs := make(chan amqp.Delivery, 2)
	shutdown := make(chan struct{})

	recorder := &ackRecorder{}

	go func() {
		shutdownSent := false
		for msg := range msgs {
			var update VersionUpdate
			if err := json.Unmarshal(msg.Body, &update); err != nil {
				continue
			}
			newVer, _ := semver.NewVersion(update.Version)
			if newVer.GreaterThan(currentVer) && !shutdownSent {
				_ = msg.Ack(false)
				close(shutdown)
				shutdownSent = true
				continue
			}
			_ = msg.Ack(false)
		}
	}()

	newerBody, _ := json.Marshal(VersionUpdate{Version: "1.4.0", Timestamp: time.Now()})
	msgs <- amqp.Delivery{Acknowledger: recorder, Body: newerBody, DeliveryTag: 1}
	olderBody, _ := json.Marshal(VersionUpdate{Version: "1.2.0", Timestamp: time.Now()})
	msgs <- amqp.Delivery{Acknowledger: recorder, Body: olderBody, DeliveryTag: 2}
	close(msgs)

	<-shutdown

	if len(recorder.acks) != 2 {
		t.Fatalf("expected both messages to be acked, got %d", len(recorder.acks))
	}
	if recorder.acks[0] != 1 || recorder.acks[1] != 2 {
		t.Fatalf("unexpected ack order: %+v", recorder.acks)
	}
}
