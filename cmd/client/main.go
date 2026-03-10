package main

import (
	"flag"
	"fmt"
	"os"

	"netembeddb"
)

type User struct {
	ID     uint32
	Name   string
	Email  string
	Age    int
	Active bool
}

func main() {
	addr := flag.String("addr", "localhost:9876", "Server address")
	authKey := flag.String("auth-key", "", "Authentication key")
	flag.Parse()

	// Load auth key from env if not provided
	if *authKey == "" {
		*authKey = os.Getenv("NETEMBEDDB_AUTH_KEY")
	}

	fmt.Printf("Connecting to %s...\n", *addr)

	client, err := netembeddb.Connect(*addr, *authKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Println("Connected!")

	// Register table
	err = client.RegisterTable("users", User{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to register table: %v\n", err)
		os.Exit(1)
	}

	// Insert a record
	user := &User{
		Name:   "Alice",
		Email:  "alice@example.com",
		Age:    30,
		Active: true,
	}

	id, err := client.Insert("users", user)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to insert: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Inserted user with ID: %d\n", id)

	// Query
	ids, err := client.Query("users", "Age", 30)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to query: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Found %d users with age 30\n", len(ids))

	// Count
	count, err := client.Count("users")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to count: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Total users: %d\n", count)

	fmt.Println("Done!")
}
