package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoDBAPI interface {
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func GetDataFromDynamoDB(client DynamoDBAPI, tableName, id string) ([]map[string]string, error) {
	exprAttrVals, err := attributevalue.MarshalMap(map[string]string{":id": id})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the dynamodb key: %w", err)
	}
	keyConditionExpression := "id=:id"
	resp, err := client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    &keyConditionExpression,
		ExpressionAttributeValues: exprAttrVals,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Items) == 0 {
		return nil, nil
	}
	items := make([]map[string]string, len(resp.Items))
	if err := attributevalue.UnmarshalListOfMaps(resp.Items, &items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal dynamodb items to slice of map: %w", err)
	}
	return items, nil
}

// OpenCsvFileForAppending
func OpenCsvFileForAppending(outputPath, fileName string) *os.File {
	fullFileName := ""
	if outputPath != "" {
		if err := os.MkdirAll(outputPath, os.ModePerm); err != nil {
			panic(err)
		}
		fullFileName = fmt.Sprintf("%s/%s.csv", outputPath, fileName)
	} else {
		fullFileName = fmt.Sprintf("%s.csv", fileName)
	}

	file, err := os.OpenFile(fullFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return file
}

// WriteCSV
func WriteCSV(formattedOutput [][]string, wr io.Writer) {
	writer := csv.NewWriter(wr)
	defer writer.Flush()

	for i := range formattedOutput {
		err := writer.Write(formattedOutput[i])
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	ddbTableName := ""
	ddbItemsHash := ""

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load AWS config, %v", err)
	}
	ddb := dynamodb.NewFromConfig(cfg)
	items, err := GetDataFromDynamoDB(ddb, ddbTableName, ddbItemsHash)
	if err != nil {
		log.Fatalf("failed to query data from dynamodb: %w", err)
	}

	header := []string{"id", "column2", "column3"}
	csvSlice := [][]string{header}
	for _, item := range items {
		row := make([]string, len(header))
		for _, col := range header {
			if v, ok := item[col]; !ok {
				log.Fatalf("key '%s' not found in item %v", col, item)
			} else {
				row = append(row, v)
			}
		}
		csvSlice = append(csvSlice, row)
	}

	file := OpenCsvFileForAppending("", "test")
	defer file.Close()

	WriteCSV(csvSlice, file)
}
