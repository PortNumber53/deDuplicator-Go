package cmd

import (
	"database/sql"
	"fmt"
	"strings"

	"deduplicator/db"
)

// HandleManage handles the manage command
func HandleManage(dbConn *sql.DB, args []string) error {
	if len(args) < 1 {
		cmd := FindCommand("manage")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("manage command requires a subcommand")
	}

	if args[0] == "help" || args[0] == "--help" {
		cmd := FindCommand("manage")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	subcommand := args[0]

	switch subcommand {
	case "server-list":
		hosts, err := db.ListHosts(dbConn)
		if err != nil {
			return fmt.Errorf("error listing servers: %v", err)
		}
		if len(hosts) == 0 {
			fmt.Println("No servers found. Use 'deduplicator manage server-add' to add a server.")
			return nil
		}
		fmt.Printf("%-20s %-30s %-15s\n", "NAME", "HOSTNAME", "IP")
		fmt.Println(strings.Repeat("-", 70))
		for _, host := range hosts {
			fmt.Printf("%-20s %-30s %-15s\n", host.Name, host.Hostname, host.IP)
		}
		return nil

	case "server-add":
		if len(args) < 2 {
			fmt.Println("Usage: deduplicator manage server-add \"Friendly server name\" --hostname <hostname> --ip <ip>")
			return nil
		}
		name := args[1]
		hostname := ""
		ip := ""
		for i := 2; i < len(args); i++ {
			if args[i] == "--hostname" && i+1 < len(args) {
				hostname = args[i+1]
				i++
			} else if args[i] == "--ip" && i+1 < len(args) {
				ip = args[i+1]
				i++
			}
		}
		if hostname == "" {
			fmt.Println("--hostname is required")
			return nil
		}
		if err := db.AddHost(dbConn, name, hostname, ip, "", nil); err != nil {
			return fmt.Errorf("error adding server: %v", err)
		}
		fmt.Printf("Server '%s' added successfully\n", name)
		return nil

	case "server-edit":
		if len(args) < 2 {
			fmt.Println("Usage: deduplicator manage server-edit \"Current friendly name\" [--servername <new friendly name>] [--hostname <hostname>] [--ip <ip>]")
			return nil
		}
		currentName := args[1]
		newName := ""
		hostname := ""
		ip := ""
		for i := 2; i < len(args); i++ {
			if args[i] == "--servername" && i+1 < len(args) {
				newName = args[i+1]
				i++
			} else if args[i] == "--hostname" && i+1 < len(args) {
				hostname = args[i+1]
				i++
			} else if args[i] == "--ip" && i+1 < len(args) {
				ip = args[i+1]
				i++
			}
		}
		host, err := db.GetHost(dbConn, currentName)
		if err != nil {
			return fmt.Errorf("error fetching server: %v", err)
		}
		if hostname == "" {
			hostname = host.Hostname
		}
		if ip == "" {
			ip = host.IP
		}
		if newName == "" {
			newName = host.Name
		}
		if err := db.UpdateHost(dbConn, currentName, newName, hostname, ip, host.RootPath, host.Settings); err != nil {
			return fmt.Errorf("error updating server: %v", err)
		}
		fmt.Printf("Server '%s' updated successfully\n", newName)
		return nil

	case "server-delete":
		if len(args) != 2 {
			fmt.Println("Usage: deduplicator manage server-delete \"Friendly server name\"")
			return nil
		}
		name := args[1]
		if err := db.DeleteHost(dbConn, name); err != nil {
			return fmt.Errorf("error deleting server: %v", err)
		}
		fmt.Printf("Server '%s' deleted successfully\n", name)
		return nil

	case "path-list":
		if len(args) != 2 {
			fmt.Println("Usage: deduplicator manage path-list <server name>")
			return nil
		}
		host, err := db.GetHost(dbConn, args[1])
		if err != nil {
			return fmt.Errorf("error fetching server: %v", err)
		}
		paths, err := host.GetPaths()
		if err != nil {
			return fmt.Errorf("error decoding paths: %v", err)
		}
		if len(paths) == 0 {
			fmt.Println("No paths found for this server.")
			return nil
		}
		fmt.Printf("%-20s %s\n", "FRIENDLY NAME", "ABSOLUTE PATH")
		fmt.Println(strings.Repeat("-", 60))
		for friendly, abs := range paths {
			fmt.Printf("%-20s %s\n", friendly, abs)
		}
		return nil

	case "path-add":
		if len(args) != 4 {
			fmt.Println("Usage: deduplicator manage path-add <server name> <friendly path name> <absolute path>")
			return nil
		}
		serverName, friendly, abs := args[1], args[2], args[3]
		host, err := db.GetHost(dbConn, serverName)
		if err != nil {
			return fmt.Errorf("error fetching server: %v", err)
		}
		paths, err := host.GetPaths()
		if err != nil {
			return fmt.Errorf("error decoding paths: %v", err)
		}
		paths[friendly] = abs
		host.SetPaths(paths)
		if err := db.UpdateHost(dbConn, host.Name, host.Name, host.Hostname, host.IP, host.RootPath, host.Settings); err != nil {
			return fmt.Errorf("error updating paths: %v", err)
		}
		fmt.Printf("Path '%s' added to server '%s'\n", friendly, serverName)
		return nil

	case "path-delete":
		if len(args) != 3 {
			fmt.Println("Usage: deduplicator manage path-delete <server name> <friendly path name>")
			return nil
		}
		serverName, friendly := args[1], args[2]
		host, err := db.GetHost(dbConn, serverName)
		if err != nil {
			return fmt.Errorf("error fetching server: %v", err)
		}
		paths, err := host.GetPaths()
		if err != nil {
			return fmt.Errorf("error decoding paths: %v", err)
		}
		if _, ok := paths[friendly]; !ok {
			fmt.Printf("Path '%s' not found for server '%s'\n", friendly, serverName)
			return nil
		}
		delete(paths, friendly)
		host.SetPaths(paths)
		if err := db.UpdateHost(dbConn, host.Name, host.Name, host.Hostname, host.IP, host.RootPath, host.Settings); err != nil {
			return fmt.Errorf("error updating paths: %v", err)
		}
		fmt.Printf("Path '%s' deleted from server '%s'\n", friendly, serverName)
		return nil

	case "path-edit":
		if len(args) != 4 {
			fmt.Println("Usage: deduplicator manage path-edit <server name> <friendly path name> <new absolute path>")
			return nil
		}
		serverName, friendly, newAbs := args[1], args[2], args[3]
		host, err := db.GetHost(dbConn, serverName)
		if err != nil {
			return fmt.Errorf("error fetching server: %v", err)
		}
		paths, err := host.GetPaths()
		if err != nil {
			return fmt.Errorf("error decoding paths: %v", err)
		}
		if _, ok := paths[friendly]; !ok {
			fmt.Printf("Path '%s' not found for server '%s'\n", friendly, serverName)
			return nil
		}
		paths[friendly] = newAbs
		host.SetPaths(paths)
		if err := db.UpdateHost(dbConn, host.Name, host.Name, host.Hostname, host.IP, host.RootPath, host.Settings); err != nil {
			return fmt.Errorf("error updating paths: %v", err)
		}
		fmt.Printf("Path '%s' updated for server '%s'\n", friendly, serverName)
		return nil

	default:
		fmt.Println("Unknown or unsupported manage subcommand.")
		return nil
	}
}

