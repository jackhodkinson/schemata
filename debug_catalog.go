package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
)

func main() {
	ctx := context.Background()

	// Connect to dev database
	devConn := &config.DBConnection{
		URL: stringPtr("postgresql://postgres:dev_password@localhost:54320/dev_db"),
	}

	pool, err := db.Connect(ctx, devConn)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	// Extract objects
	catalog := db.NewCatalog(pool)
	objects, err := catalog.ExtractAllObjects(ctx, nil, nil)
	if err != nil {
		log.Fatalf("Failed to extract objects: %v", err)
	}

	fmt.Printf("Found %d objects:\n\n", len(objects))
	for i, obj := range objects {
		fmt.Printf("%d. %T: %+v\n", i+1, obj, obj)
	}
}

func stringPtr(s string) *string {
	return &s
}
