package files

import (
	"context"
	"database/sql"
)

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/schollz/progressbar/v3"
	"deduplicator/logging"
)

// hostPath describes a host and its absolute path for the friendly path
//
type hostPath struct {
	Name      string
	Hostname  string
	RootPath  string
	AbsPath   string
}

type conflictEntry struct {
	RelPath string
	Hosts   []string
	Hashes  []string
	Reason  string
}

// MirrorFriendlyPath syncs files across all hosts that have the same friendly path registered.
func MirrorFriendlyPath(ctx context.Context, db *sql.DB, friendlyPath string) error {
	// 1. Find all hosts with the friendly path
	hosts, err := getHostsForFriendlyPath(db, friendlyPath)
	if err != nil {
		return fmt.Errorf("error fetching hosts for friendly path: %w", err)
	}
	if len(hosts) < 2 {
		return fmt.Errorf("need at least 2 hosts with this friendly path to mirror")
	}

	// 2. Build file hash maps for each host
	hostFiles := make(map[string]map[string]string) // hostname -> relpath -> hash
	allRelPaths := map[string]struct{}{}
	for _, h := range hosts {
		files, err := getFilesForHostPath(db, h)
		if err != nil {
			return fmt.Errorf("error fetching files for host %s: %w", h.Hostname, err)
		}
		hostFiles[h.Hostname] = files
		for rel := range files {
			allRelPaths[rel] = struct{}{}
		}
	}

	// 3. Aggregate, compare, and sync
	var conflicts []conflictEntry
	var copies []string

	// Build a slice of all transfers to do (for progress bar)
	type transferTask struct {
		relPath string
		srcHost hostPath
		dstHost hostPath
		hashVal string
	}
	var tasks []transferTask
	for relPath := range allRelPaths {
		present := map[string]string{} // hostname -> hash
		missing := []hostPath{}
		for _, h := range hosts {
			hash, ok := hostFiles[h.Hostname][relPath]
			if ok {
				present[h.Hostname] = hash
			} else {
				missing = append(missing, h)
			}
		}
		// Check for hash conflicts
		hashSet := map[string]struct{}{}
		for _, hash := range present {
			hashSet[hash] = struct{}{}
		}
		if len(hashSet) > 1 {
			// Conflict: different hashes for same relPath
			var hostsList, hashesList []string
			for host, hash := range present {
				hostsList = append(hostsList, host)
				hashesList = append(hashesList, hash)
			}
			conflicts = append(conflicts, conflictEntry{
				RelPath: relPath,
				Hosts: hostsList,
				Hashes: hashesList,
				Reason: "hash mismatch",
			})
			continue
		}
		if len(present) == 0 || len(missing) == 0 {
			continue // nothing to do
		}
		// All present hashes are the same
		var hashVal string
		for _, v := range present {
			hashVal = v
			break
		}
		// Pick a source host (first present)
		var srcHost hostPath
		for _, h := range hosts {
			if _, ok := present[h.Hostname]; ok {
				srcHost = h
				break
			}
		}
		for _, dst := range missing {
			tasks = append(tasks, transferTask{
				relPath: relPath,
				srcHost: srcHost,
				dstHost: dst,
				hashVal: hashVal,
			})
		}
	}
	bar := progressbar.Default(int64(len(tasks)), "Mirroring files")

	for _, task := range tasks {
		relPath := task.relPath
		srcHost := task.srcHost
		dst := task.dstHost
		hashVal := task.hashVal
		// Check if file exists on destination's file system (using ssh)
		absDst := strings.TrimRight(dst.AbsPath, "/") + "/" + relPath
		cmd := exec.CommandContext(ctx, "ssh", dst.Hostname, "test", "-e", absDst)
		err := cmd.Run()
		if err == nil {
			// File exists on disk but not in DB: log conflict
			conflicts = append(conflicts, conflictEntry{
				RelPath: relPath,
				Hosts: []string{dst.Hostname},
				Hashes: []string{"n/a"},
				Reason: "file exists on disk but not in DB",
			})
			_ = bar.Add(1)
			continue
		}
		// Ensure parent directory exists on destination
		parentDir := absDst[:strings.LastIndex(absDst, "/")]
		mkdirCmd := exec.CommandContext(ctx, "ssh", dst.Hostname, "mkdir", "-p", parentDir)
		logging.InfoLogger.Printf("Ensuring directory on %s: %s", dst.Hostname, parentDir)
		if mkErr := mkdirCmd.Run(); mkErr != nil {
			logging.ErrorLogger.Printf("Failed to create parent directory on %s: %v", dst.Hostname, mkErr)
			conflicts = append(conflicts, conflictEntry{
				RelPath: relPath,
				Hosts: []string{dst.Hostname},
				Hashes: []string{"n/a"},
				Reason: fmt.Sprintf("mkdir failed: %v", mkErr),
			})
			_ = bar.Add(1)
			continue
		}

		srcAbs := strings.TrimRight(srcHost.AbsPath, "/") + "/" + relPath
		dstAbs := absDst

		localHost, _ := os.Hostname()
		localIsSrc := strings.EqualFold(localHost, srcHost.Hostname)
		if localIsSrc {
			// Local is source: rsync local to remote
			rsyncCmd := fmt.Sprintf("rsync %s %s:%s", srcAbs, dst.Hostname, dstAbs)
			logging.InfoLogger.Printf("Running: %s", rsyncCmd)
			copyCmd := exec.CommandContext(ctx, "rsync", srcAbs, dst.Hostname+":"+dstAbs)
			copyErr := copyCmd.Run()
			if copyErr != nil {
				conflicts = append(conflicts, conflictEntry{
					RelPath: relPath,
					Hosts: []string{srcHost.Hostname, dst.Hostname},
					Hashes: []string{hashVal},
					Reason: fmt.Sprintf("rsync failed: %v", copyErr),
				})
			} else {
				copies = append(copies, fmt.Sprintf("%s -> %s: %s", srcHost.Hostname, dst.Hostname, relPath))
			}
			_ = bar.Add(1)
		} else {
			// Orchestrator is not source: pull to tmp, then push
			tmpPath := "/tmp/mirror-tmp-" + hashVal
			// Pull
			pullCmdStr := fmt.Sprintf("rsync %s:%s %s", srcHost.Hostname, srcAbs, tmpPath)
			logging.InfoLogger.Printf("Running: %s", pullCmdStr)
			pullCmd := exec.CommandContext(ctx, "rsync", srcHost.Hostname+":"+srcAbs, tmpPath)
			pullErr := pullCmd.Run()
			if pullErr != nil {
				conflicts = append(conflicts, conflictEntry{
					RelPath: relPath,
					Hosts: []string{srcHost.Hostname, dst.Hostname},
					Hashes: []string{hashVal},
					Reason: fmt.Sprintf("pull failed: %v", pullErr),
				})
				_ = bar.Add(1)
				continue
			}
			// Push
			pushCmdStr := fmt.Sprintf("rsync %s %s:%s", tmpPath, dst.Hostname, dstAbs)
			logging.InfoLogger.Printf("Running: %s", pushCmdStr)
			pushCmd := exec.CommandContext(ctx, "rsync", tmpPath, dst.Hostname+":"+dstAbs)
			pushErr := pushCmd.Run()
			if pushErr != nil {
				conflicts = append(conflicts, conflictEntry{
					RelPath: relPath,
					Hosts: []string{srcHost.Hostname, dst.Hostname},
					Hashes: []string{hashVal},
					Reason: fmt.Sprintf("push failed: %v", pushErr),
				})
			} else {
				// Cleanup
				_ = os.Remove(tmpPath)
				copies = append(copies, fmt.Sprintf("%s -> %s: %s", srcHost.Hostname, dst.Hostname, relPath))
			}
			_ = bar.Add(1)
		}
	}
	// Log summary
	if len(copies) > 0 {
		logging.InfoLogger.Printf("Files copied:")
		for _, c := range copies {
			logging.InfoLogger.Printf("%s", c)
		}
	} else {
		logging.InfoLogger.Printf("No files copied.")
	}
	if len(conflicts) > 0 {
		logging.ErrorLogger.Printf("Conflicts:")
		for _, conf := range conflicts {
			logging.ErrorLogger.Printf("%s: %s | hosts: %v | hashes: %v", conf.RelPath, conf.Reason, conf.Hosts, conf.Hashes)
		}
	} else {
		logging.InfoLogger.Printf("No conflicts detected.")
	}
	return nil
}

// getHostsForFriendlyPath returns hosts and the absolute path for the friendly path
func getHostsForFriendlyPath(db *sql.DB, friendlyPath string) ([]hostPath, error) {
	rows, err := db.Query("SELECT name, hostname, root_path, settings FROM hosts")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []hostPath
	for rows.Next() {
		var name, hostname, rootPath string
		var settingsRaw []byte
		if err := rows.Scan(&name, &hostname, &rootPath, &settingsRaw); err != nil {
			return nil, err
		}
		var settings struct {
			Paths map[string]string `json:"paths"`
		}
		if err := json.Unmarshal(settingsRaw, &settings); err != nil {
			continue // skip hosts with bad json
		}
		abs, ok := settings.Paths[friendlyPath]
		if ok {
			result = append(result, hostPath{
				Name:     name,
				Hostname: hostname,
				RootPath: rootPath,
				AbsPath:  abs,
			})
		}
	}
	return result, nil
}

// getFilesForHostPath returns relative path -> hash for a given host/path
func getFilesForHostPath(db *sql.DB, h hostPath) (map[string]string, error) {
	q := `SELECT path, hash FROM files WHERE hostname = $1 AND root_folder = $2 AND hash IS NOT NULL`
	rows, err := db.Query(q, h.Hostname, h.AbsPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			return nil, err
		}
		result[path] = hash
	}
	return result, nil
}
