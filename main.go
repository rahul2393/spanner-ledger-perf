package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
)

var (
	tableWithForeignKeys = []string{
		`CREATE TABLE USER_BAL (
				UserId INT64 NOT NULL,
				Balance FLOAT64 NOT NULL,
				CreationTime TIMESTAMP NOT NULL
			) PRIMARY KEY (UserId)`,
		`CREATE TABLE USER_TXNS (
				UserId INT64 NOT NULL,
				TransactionId INT64 NOT NULL,
				Amount FLOAT64 NOT NULL,
				CreationTime TIMESTAMP NOT NULL,
				CONSTRAINT FK_USER_BAL FOREIGN KEY (UserId) REFERENCES USER_BAL (UserId)
			) PRIMARY KEY (TransactionId)`,
	}
	// TODO: use with index on transaction
	// Add thread ID in txns
	// Check writes happening
	// Write/Read lock
	// How to take pessimistic lock in spanner

	tableWithInterleaving = []string{
		`CREATE TABLE USER_BAL (
				UserId INT64 NOT NULL,
				Balance FLOAT64 NOT NULL,
				CreationTime TIMESTAMP NOT NULL
			) PRIMARY KEY (UserId)`,
		`CREATE TABLE USER_TXNS (
				UserId INT64 NOT NULL,
				TransactionId INT64 NOT NULL,
				Amount FLOAT64 NOT NULL,
				CreationTime TIMESTAMP NOT NULL
			) PRIMARY KEY (UserId, TransactionId),
  			INTERLEAVE IN PARENT USER_BAL ON DELETE CASCADE`,
	}
	projectID       string
	instanceID      string
	dbName          string
	numUsers        int64
	numTxn          int64
	useInterleaving bool
	cleanup         bool
)

func init() {
	flag.StringVar(&projectID, "project_id", "span-cloud-testing", "Spanner ProjectID")
	flag.StringVar(&instanceID, "instance_id", "spanner-testing", "Spanner InstanceID")
	flag.StringVar(&dbName, "db_name", "glance_ledger", "Spanner DB")
	flag.Int64Var(&numUsers, "num_users", 1, "Number of concurrent users to run ledger")
	flag.Int64Var(&numTxn, "num_txn", 20, "Number of concurrent txns to run ledger")
	flag.BoolVar(&useInterleaving, "use_interleave", false, "Spanner use interleaving")
	flag.BoolVar(&cleanup, "do_cleanup", false, "Spanner do cleanup")
}

func main() {
	flag.Parse()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	ctx := context.Background()
	dbPath := fmt.Sprintf("projects/%v/instances/%v/databases/%v", projectID, instanceID, dbName)
	// created the datbase
	databaseAdmin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}
	defer databaseAdmin.Close()

	client, err := spanner.NewClientWithConfig(ctx, dbPath,
		spanner.ClientConfig{SessionPoolConfig: spanner.DefaultSessionPoolConfig})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if useInterleaving {
		createDatabase(databaseAdmin, tableWithInterleaving)
	} else {
		createDatabase(databaseAdmin, tableWithForeignKeys)
	}
	insertUsers(ctx, client)
	for i := int64(1); i <= numUsers; i++ {
		go func(userID int64, client *spanner.Client) {
			for {
				runTxns(context.Background(), client, userID)
			}
		}(i, client)
	}
	select {
	case <-c:
		if cleanup {
			deleteDatabase(databaseAdmin, projectID, instanceID)
		}
	}
}

func createDatabase(databaseAdmin *database.DatabaseAdminClient, ddlstmts []string) {
	ctx := context.Background()
	// Create database and tables.
	req := &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%v/instances/%v", projectID, instanceID),
		CreateStatement: "CREATE DATABASE " + dbName,
		ExtraStatements: ddlstmts,
	}
	op, err := databaseAdmin.CreateDatabaseWithRetry(ctx, req)
	if err != nil {
		log.Fatalf("cannot create testing DB %v: %v", dbName, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		log.Fatalf("cannot create testing DB %v: %v", dbName, err)
	}
}

func deleteDatabase(databaseAdmin *database.DatabaseAdminClient, projectID, instanceID string) {
	fmt.Printf("Deleting DB")
	ctx := context.Background()
	iter := databaseAdmin.ListDatabases(ctx, &adminpb.ListDatabasesRequest{
		Parent:   fmt.Sprintf("projects/%v/instances/%v", projectID, instanceID),
		PageSize: 0,
	})
	for {
		db, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if strings.HasPrefix(db.Name, fmt.Sprintf("projects/span-cloud-testing/instances/spanner-testing/databases/%v", dbName)) {
			fmt.Printf("db name is %v\n", db.Name)
			if err := databaseAdmin.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: db.Name}); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func insertUsers(ctx context.Context, client *spanner.Client) {
	users := []*spanner.Mutation{}
	for i := int64(1); i <= numUsers; i++ {
		users = append(users, spanner.Insert("USER_BAL", []string{"UserId", "Balance", "CreationTime"}, []interface{}{i, float64(0), time.Now()}))
	}
	if _, err := client.Apply(ctx, users, spanner.ApplyAtLeastOnce()); err != nil {
		log.Fatal(err)
	}
}

func runTxns(ctx context.Context, client *spanner.Client, userID int64) {
	readBalance := func(iter *spanner.RowIterator) (float64, error) {
		defer iter.Stop()
		var bal float64
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				return bal, nil
			}
			if err != nil {
				return 0, err
			}
			if err := row.Column(0, &bal); err != nil {
				return 0, err
			}
		}
	}
	queryBalanceByUserID := "SELECT Balance FROM USER_BAL WHERE UserId = @p1"

	wg := sync.WaitGroup{}
	for i := int64(0); i < numTxn; i++ {
		wg.Add(1)
		go func(iter int64) {
			defer wg.Done()
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				// Query Foo's balance and Bar's balance.
				bf, e := readBalance(tx.Query(ctx,
					spanner.Statement{queryBalanceByUserID, map[string]interface{}{"p1": int64(userID)}}))
				if e != nil {
					return e
				}
				bf++
				rand.Seed(time.Now().UnixNano())
				min := 1
				max := 9000000000
				return tx.BufferWrite([]*spanner.Mutation{
					spanner.Insert("USER_TXNS", []string{"UserId", "TransactionId", "Amount", "CreationTime"}, []interface{}{int64(userID), int64(rand.Intn(max-min) + min), float64(1), time.Now()}),
					spanner.Update("USER_BAL", []string{"UserId", "Balance"}, []interface{}{int64(userID), bf}),
				})
			})
			if err != nil {
				log.Fatalf("%d: failed to execute transaction: %v", iter, err)
			}
		}(i)
	}
	wg.Wait()
}
