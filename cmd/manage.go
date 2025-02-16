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
		return fmt.Errorf("manage command requires a subcommand: add, edit, delete, or list")
	}

	if args[0] == "help" {
		cmd := FindCommand("manage")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	subcommand := args[0]
	switch subcommand {
	case "list":
		hosts, err := db.ListHosts(dbConn)
		if err != nil {
			return fmt.Errorf("error listing hosts: %v", err)
		}
		if len(hosts) == 0 {
			fmt.Println("No hosts found. Use 'dedupe manage add' to add a host.")
			return nil
		}
		fmt.Printf("%-20s %-30s %-15s %s\n", "NAME", "HOSTNAME", "IP", "ROOT PATH")
		fmt.Println(strings.Repeat("-", 80))
		for _, host := range hosts {
			fmt.Printf("%-20s %-30s %-15s %s\n", host.Name, host.Hostname, host.IP, host.RootPath)
		}
		return nil

	case "add", "edit":
		if len(args) != 5 {
			fmt.Printf("Usage: dedupe manage %s <name> <hostname> <ip> <root_path>\n", subcommand)
			fmt.Printf("\nExample:\n  dedupe manage %s myhost example.com 192.168.1.100 /data\n", subcommand)
			return nil
		}
		name, hostname, ip, rootPath := args[1], args[2], args[3], args[4]

		if subcommand == "add" {
			err := db.AddHost(dbConn, name, hostname, ip, rootPath)
			if err != nil {
				return fmt.Errorf("error adding host: %v", err)
			}
			fmt.Printf("Host '%s' added successfully\n", name)
		} else {
			err := db.UpdateHost(dbConn, name, hostname, ip, rootPath)
			if err != nil {
				return fmt.Errorf("error updating host: %v", err)
			}
			fmt.Printf("Host '%s' updated successfully\n", name)
		}
		return nil

	case "delete":
		if len(args) != 2 {
			fmt.Println("Usage: dedupe manage delete <name>")
			fmt.Println("\nExample:\n  dedupe manage delete myhost")
			return nil
		}
		name := args[1]
		err := db.DeleteHost(dbConn, name)
		if err != nil {
			return fmt.Errorf("error deleting host: %v", err)
		}
		fmt.Printf("Host '%s' deleted successfully\n", name)
		return nil

	default:
		return fmt.Errorf("unknown subcommand: %s", subcommand)
	}
}
