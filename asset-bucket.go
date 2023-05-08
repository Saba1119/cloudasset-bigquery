package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func main() {
	// Set up authentication credentials
	ctx := context.Background()
	credsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credsPath == "" {
		log.Fatal("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
	}

	// Create clients for Google Cloud Storage and BigQuery
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer storageClient.Close()

	projectID := "develop-375210"
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer bqClient.Close()

	// Query for Google Cloud Storage buckets
	it := storageClient.Buckets(ctx, projectID)
	var buckets []string
	for {
		bucketAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		buckets = append(buckets, bucketAttrs.Name)
	}

	// Insert the results into BigQuery
	datasetID := "test"
	tableID := "cloudstorage"

	dataset := bqClient.Dataset(datasetID)
	table := dataset.Table(tableID)
	u := table.Uploader()

	for _, bucket := range buckets {
		var row struct {
			Name     string `bigquery:"name"`
			Location string `bigquery:"location"`
			Created  string `bigquery:"created"`
		}
		row.Name = fmt.Sprintf("gs://%s", bucket)

		// Get the bucket attributes to extract the location and creation and update times
		bucketAttrs, err := storageClient.Bucket(bucket).Attrs(ctx)
		if err != nil {
			log.Fatal(err)
		}
		row.Location = bucketAttrs.Location
		row.Created = bucketAttrs.Created.Format("2006-01-02 15:04:05")
		if err := u.Put(ctx, row); err != nil {
			log.Fatal(err)
		}
	}
}
