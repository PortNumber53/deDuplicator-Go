package cmd

import (
	"context"
	"fmt"
	"log"

	"deduplicator/mq"
)

// HandleQueueVersion handles the queue version command
func HandleQueueVersion(ctx context.Context, rabbit *mq.RabbitMQ, version string, currentVersion string) error {
	if version == currentVersion {
		log.Printf("Publishing current version: %s", currentVersion)
	} else {
		log.Printf("Warning: Publishing version %s which differs from current version %s",
			version, currentVersion)
	}

	if err := rabbit.PublishVersionUpdate(ctx, version); err != nil {
		return fmt.Errorf("failed to publish version update: %v", err)
	}

	return nil
}
