package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"netembeddb"
)

func main() {
	addr := flag.String("addr", ":9876", "Address to listen on (e.g., :9876 or /tmp/mydb.sock)")
	dataDir := flag.String("data-dir", ".", "Directory for database files")
	authKey := flag.String("auth-key", "", "Shared authentication key (empty for no auth)")
	flag.Parse()

	// Load auth key from env if not provided
	if *authKey == "" {
		*authKey = os.Getenv("NETEMBEDDB_AUTH_KEY")
	}

	fmt.Printf("Starting netembeddb server...\n")
	fmt.Printf("  Listen: %s\n", *addr)
	fmt.Printf("  Data dir: %s\n", *dataDir)
	if *authKey != "" {
		fmt.Printf("  Auth: enabled\n")
	} else {
		fmt.Printf("  Auth: disabled\n")
	}

	server := netembeddb.NewServer(*dataDir, *authKey)

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		server.Close()
		os.Exit(0)
	}()

	fmt.Printf("Server listening on %s\n", *addr)
	if err := server.Listen(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
