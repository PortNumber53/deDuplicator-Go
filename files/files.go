package files

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/schollz/progressbar/v3"
)

// calculateFileHash computes the SHA-256 hash of a file
func calculateFileHash(filePath string) (string, error) {
	// Create a channel to communicate the result and progress
	resultCh := make(chan struct {
		hash string
		err  error
	}, 1)

	// Create a context with cancellation for manual control
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a channel to track progress
	progressCh := make(chan struct{}, 1)

	// Start a goroutine to monitor progress and implement timeout
	go func() {
		timeout := 1 * time.Minute
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		for {
			select {
			case <-progressCh:
				// Progress was made, reset the timer
				if !timer.Stop() {
					<-timer.C // Drain the channel if timer already fired
				}
				timer.Reset(timeout)
			case <-timer.C:
				// Timeout occurred with no progress
				cancel() // Cancel the context to stop the hashing
				return
			case <-ctx.Done():
				// Context was cancelled elsewhere or operation completed
				return
			}
		}
	}()

	// Run the hashing in a goroutine
	go func() {
		hash, err := calculateFileHashInternal(ctx, filePath, progressCh)
		resultCh <- struct {
			hash string
			err  error
		}{hash, err}
	}()

	// Wait for the result
	select {
	case result := <-resultCh:
		return result.hash, result.err
	case <-ctx.Done():
		return "", fmt.Errorf("hashing timed out after 1 minute of inactivity for file: %s", filePath)
	}
}

// calculateFileHashInternal is the internal implementation of file hashing
func calculateFileHashInternal(ctx context.Context, filePath string, progressCh chan struct{}) (string, error) {
	// Use Lstat instead of Stat to detect symlinks without following them
	fileInfo, err := os.Lstat(filePath)
	if err != nil {
		return "", fmt.Errorf("error accessing file: %v", err)
	}

	// Check for directories
	if fileInfo.IsDir() {
		return "", fmt.Errorf("path is a directory")
	}

	// Check for symlinks
	if fileInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("path is a symlink")
	}

	// Check for device files, pipes, sockets, etc.
	if fileInfo.Mode()&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
		return "", fmt.Errorf("path is a device file, pipe, or socket")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Create a progress bar for this file
	bar := progressbar.NewOptions64(fileInfo.Size(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]Hashing %s", filepath.Base(filePath))),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	hash := sha256.New()
	reader := bufio.NewReader(file)
	buf := make([]byte, 1024*1024) // 1MB buffer

	for {
		// Check if context is cancelled before reading
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("hashing operation cancelled")
		default:
		}

		n, err := reader.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
			bar.Add64(int64(n))

			// Signal progress was made
			select {
			case progressCh <- struct{}{}:
				// Progress signal sent
			default:
				// Channel buffer is full, which is fine
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	fmt.Println() // Add newline after progress bar
	return hex.EncodeToString(hash.Sum(nil)), nil
}
