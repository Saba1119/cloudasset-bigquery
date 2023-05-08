package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "cloud.google.com/go/asset/apiv1"
    "cloud.google.com/go/bigquery"
    "google.golang.org/api/iterator"
    "google.golang.org/api/option"
    assetpb "google.golang.org/genproto/googleapis/cloud/asset/v1"
)

func main() {
    projectID := "develop-375210"
    datasetID := "test"
    tableID := "vpc"

    ctx := context.Background()

    // Set up credentials for authentication
    credsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credsPath == "" {
        log.Fatal("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    }
    opts := option.WithCredentialsFile(credsPath)

    // Create the Cloud Asset API client
    assetClient, err := asset.NewClient(ctx, opts)
    if err != nil {
        panic(err)
    }
    defer assetClient.Close()

    // Create the BigQuery client
    bqClient, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        panic(err)
    }
    defer bqClient.Close()

    // Query for VPC networks
    req := &assetpb.ListAssetsRequest{
        Parent:     fmt.Sprintf("projects/%s", projectID),
        AssetTypes: []string{"compute.googleapis.com/Network"},
    }
    it := assetClient.ListAssets(ctx, req)

    // Insert the results into BigQuery
    dataset := bqClient.Dataset(datasetID)
    table := dataset.Table(tableID)
    u := table.Uploader()

    for {
        resp, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            panic(err)
        }

        var row struct {
            AssetType  string `bigquery:"asset_type"`
            AssetName  string `bigquery:"asset_name"`
            UpdateTime string `bigquery:"update_time"`
        }

        row.AssetType = resp.AssetType
        row.AssetName = resp.Name
        row.UpdateTime = resp.GetUpdateTime().AsTime().Format(time.RFC3339)

        if err := u.Put(ctx, row); err != nil {
            panic(err)
        }
    }
}







