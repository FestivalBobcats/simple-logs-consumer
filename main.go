package main

import (
	"encoding/json"
	"fmt"
	"net/http"
  "math/rand"
  "time"
  "strings"
  "os"
  "path"

  "github.com/aws/aws-sdk-go/service/kinesis"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
)

const (
  kinesisStreamName = "my-stream"
  logFileName = "logs.json"
)

func init() {
    rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[rand.Intn(len(letterRunes))]
    }
    return string(b)
}

func writeLogsToFile(logs []json.RawMessage) error {
  // Use current directory to write log file.
  dir, err := os.Getwd()
  if err != nil {
    return err
  }

  filename := path.Join(dir, logFileName)
  f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
  if err != nil {
    return err
  }

  defer f.Close()

  logOut, err := json.Marshal(logs)
  if err != nil {
    return err
  }

  if _, err = f.Write(logOut); err != nil {
    return err
  }

  return nil
}

func getKinesisClient() *kinesis.Kinesis {
  sess := session.Must(session.NewSession())
  return kinesis.New(sess)
}

func sendLogsToKinesis(logs []json.RawMessage) error {
  k := getKinesisClient()

  putRecords := []*kinesis.PutRecordsRequestEntry{}
  for _, log := range logs {
    // TODO not certain this will work for partition key without testing. Would
    // be ideal to use ID parsed from each log, if there is one.
    partitionKey := randomString(128)

    putRecord := &kinesis.PutRecordsRequestEntry{
      PartitionKey: aws.String(partitionKey),
      Data: log,
    }
    putRecords = append(putRecords, putRecord)
  }

  input := &kinesis.PutRecordsInput{
    StreamName: aws.String(kinesisStreamName),
    Records: putRecords,
  }

  out, err := k.PutRecords(input)
  if err != nil {
    return err
  }

  failedRecords := *out.FailedRecordCount
  if failedRecords > 0 {
    errorMessages := []string{}
    for _, record := range out.Records {
      errorMessages = append(errorMessages, *record.ErrorMessage)
    }
    errorMessagesStr := strings.Join(errorMessages, "\n")
    return fmt.Errorf("Kinesis submission failed for %d records:\n%s", failedRecords, errorMessagesStr)
  }

  return nil
}

func handlePostLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	if r.Body == nil {
		http.Error(w, "Request body is required", 400)
		return
	}

  // Parse logs into slice. We don't need to parse each item (yet).
	logs := []json.RawMessage{}
	err := json.NewDecoder(r.Body).Decode(logs)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

  // Send logs to Kinesis
  if err := sendLogsToKinesis(logs); err != nil {
    http.Error(w, err.Error(), 500)
    return
  }

  // Write logs to disk
  if err := writeLogsToFile(logs); err != nil {
    http.Error(w, err.Error(), 500)
    return
  }
}

func main() {
	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Welcome to my website!")
	})

	http.ListenAndServe(":8080", nil)
}
