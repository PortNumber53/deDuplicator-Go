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

// calculateFileHash computes the SHA-256 hash of a full file.
func calculateFileHash(filePath string) (string, error) {
	return calculateFileHashContext(context.Background(), filePath)
}

func calculateFileHashContext(ctx context.Context, filePath string) (string, error) {
	return calculateFileHashWithTimeout(ctx, filePath, time.Minute)
}

// The inactivity deadline remains separate from command cancellation: only
// inactivity marks a file as timed out. A blocked kernel read may finish later.
func calculateFileHashWithTimeout(parent context.Context, filePath string, timeout time.Duration) (string, error) {
	return runFileHash(parent, filePath, timeout, calculateFileHashInternal)
}

func runFileHash(parent context.Context, filePath string, timeout time.Duration, readHash func(context.Context, string, chan struct{}) (string, error)) (string, error) {
	if err := parent.Err(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	type result struct {
		hash string
		err  error
	}
	results := make(chan result, 1)
	progress := make(chan struct{}, 1)
	go func() {
		hash, err := readHash(ctx, filePath, progress)
		results <- result{hash, err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-parent.Done():
			return "", parent.Err()
		case r := <-results:
			if err := parent.Err(); err != nil {
				return "", err
			}
			return r.hash, r.err
		case <-progress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		case <-timer.C:
			if err := parent.Err(); err != nil {
				return "", err
			}
			return "", fmt.Errorf("hashing timed out after %s of inactivity for file: %s", timeout, filePath)
		}
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
	bar := newProgressBar(fileInfo.Size(), fmt.Sprintf("Hashing %s", filepath.Base(filePath)),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionFullWidth())

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

		readBuf := buf

		n, err := reader.Read(readBuf)
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

	finishProgressLine()
	return hex.EncodeToString(hash.Sum(nil)), nil
}
