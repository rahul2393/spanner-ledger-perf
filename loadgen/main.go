package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"cloud.google.com/go/spanner"
)

var (
	projectID   string
	instanceID  string
	dbName      string
	userIDDir   string
	concurrency int64

	userIds []string
)

func init() {
	flag.StringVar(&projectID, "project_id", "my-project-840969-sg", "Spanner ProjectID")
	flag.StringVar(&instanceID, "instance_id", "glance-poc", "Spanner InstanceID")
	flag.StringVar(&dbName, "db_name", "glance-db", "Spanner DB")
	flag.StringVar(&userIDDir, "user_ids_dir", "glance-gids", "Directory of folder where UserIDs CSV are stored")
	flag.Int64Var(&concurrency, "concurrency", 10, "Number of concurrent inserts to run")
}

func main() {
	flag.Parse()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	ctx := context.Background()
	dbPath := fmt.Sprintf("projects/%v/instances/%v/databases/%v", projectID, instanceID, dbName)
	client, err := spanner.NewClientWithConfig(ctx, dbPath,
		spanner.ClientConfig{SessionPoolConfig: spanner.DefaultSessionPoolConfig})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	readUserIds()
	fmt.Printf("Read %v userIds in memory", len(userIds))
	go func(client *spanner.Client) {
		for {
			runTxns(ctx, client)
		}
	}(client)
	select {
	case <-c:
		fmt.Printf("closing")
	}
}

func readUserIds() {
	if err := filepath.Walk(userIDDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) != ".csv" {
			return nil
		}
		f, _ := os.Open(path)
		defer f.Close()
		r := csv.NewReader(f)
		for {
			row, err := r.Read()
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				return err
			}
			userIds = append(userIds, row[0])
		}
	}); err != nil {
		log.Fatal(err)
	}
}

func runTxns(ctx context.Context, client *spanner.Client) {
	wg := sync.WaitGroup{}
	for i := int64(0); i < concurrency; i++ {
		wg.Add(1)
		go func(iter int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().Unix()))
			r.Intn(len(userIds))
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				count, err := tx.Update(ctx, spanner.Statement{
					SQL: `INSERT INTO user_txns_new (UserId, TransactionId, Amount, RewardType, TransactionSource, TransactionStatus, TransactionType, CreationTime) VALUES (@p1, @p2, 1, "COINS", "LOADTEST", "SUCCESS", "Credit", "2022-03-16T02:42:14.677928Z")`,
					Params: map[string]interface{}{
						"p1": userIds[r.Intn(len(userIds))],
						"p2": uuid.New().String(),
					},
				})
				if err != nil {
					return err
				}
				if count != 1 {
					log.Printf("row count: got %d, want 1", count)
				}
				return nil
			})
			if err != nil {
				log.Fatalf("%d: failed to execute transaction: %v", iter, err)
			}
		}(i)
	}
	wg.Wait()
}
