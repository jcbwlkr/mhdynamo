package mhdynamo

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/mailhog/data"
)

func TestStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("Not running storage tests in short mode")
	}

	t.Run("CRUD", CRUD)
	t.Run("DeleteAll", DeleteAll)
	t.Run("List", List)
}

// CRUD runs a series of tests to exercise Count, Store, Load, and DeleteOne operations.
func CRUD(t *testing.T) {
	t.Parallel()

	d, teardown := setup(t, "CRUD")
	defer teardown()

	if count := d.Count(); count != 0 {
		t.Errorf("At start the table has %d items, expected 0", count)
	}

	msg := &data.Message{
		ID:      mustMessageID(t),
		Created: time.Now(),
		From:    data.PathFromString("jacob@example.com"),
		To:      []*data.Path{data.PathFromString("anna@example.com")},
	}

	id, err := d.Store(msg)
	if err != nil {
		t.Fatal(err)
	}

	if count := d.Count(); count != 1 {
		t.Errorf("After storing one item the table has %d items, expected 1", count)
	}

	stored, err := d.Load(id)
	if err != nil {
		t.Errorf("Unable to load item by id %q: %v", id, err)
	}

	if diff := cmp.Diff(msg, stored); diff != "" {
		t.Errorf("Stored message different from expected:\n%s", diff)
	}

	if err := d.DeleteOne(id); err != nil {
		t.Errorf("Unable to delete item by id %q: %v", id, err)
	}

	if stored, err := d.Load(id); err == nil {
		t.Errorf("Should get error when loading deleted item %q", id)
		t.Errorf("Was able to load this: %+v", stored)
	}
}

// DeleteAll is focused on testing the DeleteAll method.
func DeleteAll(t *testing.T) {
	t.Parallel()

	d, teardown := setup(t, "DeleteAll")
	defer teardown()

	for i := 0; i < 3; i++ {
		msg := &data.Message{
			ID:      mustMessageID(t),
			Created: time.Now(),
			From:    data.PathFromString("jacob@example.com"),
			To:      []*data.Path{data.PathFromString("anna@example.com")},
		}
		if _, err := d.Store(msg); err != nil {
			t.Fatal(err)
		}
	}

	if count := d.Count(); count != 3 {
		t.Errorf("After storing items the table has %d items, expected 3", count)
	}

	if err := d.DeleteAll(); err != nil {
		t.Errorf("Unable to delete all: %v", err)
	}

	if count := d.Count(); count != 0 {
		t.Errorf("After calling DeleteAll the table has %d items, expected 0", count)
	}
}

// List is focused on testing the List method.
func List(t *testing.T) {
	t.Parallel()

	d, teardown := setup(t, "List")
	defer teardown()

	// Insert 40 messages with specially crafted creation dates so we can assert
	// the order and offset of the messages returned from List. They will be over
	// a range of dates to exercise different partition keys.
	original := makeListMessages(t, 0, 40)
	for i := range original {
		if _, err := d.Store(&original[i]); err != nil {
			t.Fatal(err)
		}
	}
	return

	// Let's say we're running these queries at noon on 2008-08-14.
	d.now = func() time.Time { return time.Date(2008, time.August, 14, 12, 0, 0, 0, time.UTC) }

	// First check we can get them all back in the right order.
	got, err := d.List(0, 40)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(&original, got); diff != "" {
		t.Errorf("got %d items", len(*got))
		t.Errorf("Listing all did not match expected:\n%s", diff)
	}

	// Next get just messages 10 through 19 (start 10, limit 10).
	got, err = d.List(10, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := makeListMessages(t, 10, 10)
	if diff := cmp.Diff(&want, got); diff != "" {
		t.Errorf("Got %d items, want %d", len(*got), len(want))
		t.Errorf("Listing only 10 did not match expected:\n%s", diff)
	}
}

func makeListMessages(t *testing.T, start, size int) data.Messages {
	now := time.Date(2008, time.August, 14, 12, 0, 0, 0, time.UTC)

	// Skip date up to the starting point.
	for i := 0; i < start; i++ {
		now = now.Add(-1 * time.Hour)
	}

	msgs := make(data.Messages, size)

	// Make up to size messages all 1 hour apart. This should give us a spread
	// that touches multiple partitions.
	for i := 0; i < size; i++ {
		id := strconv.Itoa(start + i)
		msgs[i] = data.Message{
			ID:      data.MessageID(id),
			Created: now,
			From:    data.PathFromString("jacob@example.com"),
			To:      []*data.Path{data.PathFromString("anna@example.com")},
		}
		now = now.Add(-1 * time.Hour)
	}
	return msgs
}

func setup(t *testing.T, table string) (d *Storage, teardown func()) {
	t.Helper()

	table = "test" + table

	sess, err := session.NewSession()
	if err != nil {
		t.Fatal("Could not create aws session: ", err)
	}
	db := dynamodb.New(sess)
	db.Config = *db.Config.WithLogLevel(aws.LogDebugWithHTTPBody)
	//db.AddDebugHandlers() // Turn this on to see a ton of AWS output.

	// Start creating a table for these tests. It should have Day set as the
	// Partition Key ("HASH") and ID as the Sort Key ("RANGE")
	_, err = db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(table),

		// Define which attributes make up the primary key.
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String("DayKey"),
				KeyType:       aws.String("HASH"),
			},
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("RANGE"),
			},
		},

		// Define data type for attributes: Day and ID are strings.
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("DayKey"),
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
		},

		// Set billing parameters to try to stay free-tier eligible.
		BillingMode: aws.String("PROVISIONED"),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for that table to be in the ACTIVE state. Specify a wait interval of
	// 500ms between checks otherwise the SDK will wait 20 seconds between each.
	err = db.WaitUntilTableExistsWithContext(
		context.Background(),
		&dynamodb.DescribeTableInput{TableName: aws.String(table)},
		request.WithWaiterDelay(retryInterval),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Create a DynamoDB storage with consistent reads enabled.
	d = NewStorage(db, table, true, 7)

	// Define teardown func to be called later for cleanup.
	teardown = func() {
		t.Helper()

		// Delete the table we created for these tests.
		_, err := db.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			t.Error(err)
		}
	}

	return d, teardown
}

func mustMessageID(t *testing.T) data.MessageID {
	t.Helper()
	id, err := data.NewMessageID("localhost")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
