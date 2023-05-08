package main

import (
    "context"
    "fmt"
    "os"
    "log"
    "google.golang.org/api/option"

    "cloud.google.com/go/asset/apiv1"
    "cloud.google.com/go/bigquery"
    "google.golang.org/api/iterator"
    assetpb "google.golang.org/genproto/googleapis/cloud/asset/v1"
)

func main() {
    projectID := "develop-375210"
    datasetID := "test"
    tableID := "test"

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

    // Query for compute instances
    req := &assetpb.ListAssetsRequest{
        Parent:    fmt.Sprintf("projects/%s", projectID),
        AssetTypes: []string{"compute.googleapis.com/Instance"},
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
            AssetType    string `bigquery:"asset_type"`
            AssetName    string `bigquery:"asset_name"`
        }
        row.AssetType = resp.AssetType
        row.AssetName = resp.Name
        if err := u.Put(ctx, row); err != nil {
            panic(err)
        }
    }
}
