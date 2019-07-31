package mhdynamo

// TODO(jlw) Make all of this safe for concurrent use.
// TODO(jlw) Sprinkle some context on this.
// TODO(jlw) get rid of so much nesting

import (
	"errors"
	"fmt"
	"log" // TODO(jlw) do not use the global logger.
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/mailhog/data"
)

// message is the structure we store in DynamoDB.
type message struct {
	CreatedDate string
	ID          string
	Expires     int64
	Msg         *data.Message
}

// Storage is a DynamoDB powered storage backend for MailHog.
type Storage struct {
	client     *dynamodb.DynamoDB
	table      string
	consistent bool
	ttl        int
	now        func() time.Time // Use time.Now in production and a mock for tests.
}

// NewStorage creates a DynamoDB powered storage backend that implements the
// mailhog Storage interface.
//
// The table must already be created with the keys and attributes defined in
// the README.
//
// Set consistent to true to enforce strongly consistent reads. By default
// DynamoDB is intended to be used in Eventual Consistency mode.
//
// Set ttl to a number of days that messages should be kept.
func NewStorage(client *dynamodb.DynamoDB, table string, consistent bool, ttl int) *Storage {
	// TODO(jlw) ensure ttl is positive

	return &Storage{
		client:     client,
		table:      table,
		consistent: consistent,
		ttl:        ttl,
		now:        time.Now,
	}
}

// Store stores a message in DynamoDB and returns its storage ID.
func (d *Storage) Store(m *data.Message) (string, error) {
	msg := message{
		CreatedDate: m.Created.UTC().Format(keyFormat),
		ID:          idForMsg(m),
		Expires:     m.Created.AddDate(0, 0, d.ttl).Unix(),
		Msg:         m,
	}

	// Convert the mailhog message to a dynamodb map.
	// TODO(jlw) This uses reflection and interfaces so it may be expensive. If
	// profiling shows a problem we can do the mapping ourselves but it's
	// complicated in creating each attribute with a type manually.
	item, err := dynamodbattribute.MarshalMap(msg)
	if err != nil {
		return "", err // TODO(jlw) use pkg/errors?
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.table),
		Item:      item,
	}

	if _, err = d.client.PutItem(input); err != nil {
		return "", err // TODO(jlw) on error try again with exponential backoff until a time limit
	}

	return msg.ID, nil
}

// Load loads an individual message by storage ID
func (d *Storage) Load(id string) (*data.Message, error) {
	day, err := dayForID(id)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"CreatedDate": &dynamodb.AttributeValue{S: aws.String(day)},
			"ID":          &dynamodb.AttributeValue{S: aws.String(id)},
		},
		ConsistentRead: aws.Bool(d.consistent),
	}

	output, err := d.client.GetItem(input)
	if err != nil {
		return nil, err // TODO(jlw) Implement backoff and retry
	}

	if output.Item == nil {
		return nil, fmt.Errorf("dynamodb: message %q not found", id) // TODO(jlw) pkg/errors?
	}

	var m message
	if err := dynamodbattribute.UnmarshalMap(output.Item, &m); err != nil {
		return nil, err
	}

	// NOTE: Before we return the message we have to change their messages from
	// their normal format to the dynamo format. This is a smell but the front
	// end used the value of msg.ID to make subsequent GET and DELETE requests.
	m.Msg.ID = data.MessageID(idForMsg(m.Msg))

	return m.Msg, nil
}

// DeleteOne deletes an individual message by storage ID
func (d *Storage) DeleteOne(id string) error {
	day, err := dayForID(id)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(d.table),
		Key: map[string]*dynamodb.AttributeValue{
			"CreatedDate": &dynamodb.AttributeValue{S: aws.String(day)},
			"ID":          &dynamodb.AttributeValue{S: aws.String(id)},
		},
	}

	if _, err := d.client.DeleteItem(input); err != nil {
		return err // TODO(jlw) Implement backoff and retry
	}

	return nil
}

// DeleteAll deletes all messages stored in DynamoDB.
func (d *Storage) DeleteAll() error {
	// We know all of the partition keys that should have values because they
	// have CreatedDates in the range of our TTL. Loop over those days and get the IDs
	// for that partition. Then batch delete those IDs.
	//
	// A more efficient option may be to delete the table and recreate it but we
	// can't assume to have those permissions.

	// TODO(jlw) do this concurrently

	// Call this in a loop once per day in the range
	for _, day := range daysForTTL(d.ttl, d.now()) {

		input := &dynamodb.QueryInput{
			TableName:              aws.String(d.table),
			ConsistentRead:         aws.Bool(d.consistent),
			KeyConditionExpression: aws.String("CreatedDate = :day"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":day": &dynamodb.AttributeValue{S: aws.String(day)},
			},
			ProjectionExpression: aws.String("ID"), // This is the only field we want back.
		}

		var ids []string

		// Querying a table may require multiple calls. The sdk handles this for us
		// by calling this function once per page.
		var pageErr error
		fn := func(page *dynamodb.QueryOutput, hasNext bool) bool {
			for _, item := range page.Items {
				var m message
				if err := dynamodbattribute.UnmarshalMap(item, &m); err != nil {
					err = pageErr
					return false
				}
				ids = append(ids, m.ID)
			}
			return true
		}

		if err := d.client.QueryPages(input, fn); err != nil {
			return err // TODO(jlw) retry
		}
		if pageErr != nil {
			return pageErr // TODO(jlw) retry
		}

		// Now that we have the IDs for a particular day we can batch delete them.
		// These can only be done in batches of 25 at a time.
		for len(ids) > 0 {
			end := len(ids)
			if end > 25 {
				end = 25
			}

			batch := make([]*dynamodb.WriteRequest, end)
			for i, id := range ids[:end] {
				batch[i] = &dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						// TODO(jlw) clean up all of this key stuff
						Key: map[string]*dynamodb.AttributeValue{
							"CreatedDate": &dynamodb.AttributeValue{S: aws.String(day)},
							"ID":          &dynamodb.AttributeValue{S: aws.String(id)},
						},
					},
				}
			}
			ids = ids[end:]

			input := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					d.table: batch,
				},
			}

			// Start deleting. If any UnprocessedItems come back then try again.
			for len(input.RequestItems) > 0 {
				// TODO(jlw) exponential backoff

				output, err := d.client.BatchWriteItem(input)
				if err != nil {
					return err
				}
				input.RequestItems = output.UnprocessedItems
			}
		}
	}

	return nil
}

// Count returns the number of stored messages.
func (d *Storage) Count() int {
	// NOTE: for large tables Scan (especially frequent scans) can be very slow
	// and use up all of your provisioned capacity. Consider alternatives here.
	// We could have a special partition with a single item that just stores the
	// count. Problems with that are synchronization and accounting for TTL.

	input := &dynamodb.ScanInput{
		TableName:      aws.String(d.table),
		Select:         aws.String("COUNT"),
		ConsistentRead: aws.Bool(d.consistent),
	}

	var count int64
	err := d.client.ScanPages(input, func(scan *dynamodb.ScanOutput, hasNext bool) bool {
		count += *scan.Count
		return true
	})

	// NOTE: Existing implementations ignore the error on the Count step. This is
	// generally a bad idea. If we can't count the db should we panic? Log it?
	if err != nil {
		log.Printf("could not count table: %v", err)
		return 0
	}

	return int(count)
}

// List returns a list of messages sorted by date created descending (newest
// messages first). The list will include at most limit values and will begin
// at the message indexed by start.
func (d *Storage) List(start int, limit int) (*data.Messages, error) {
	// TODO(jlw) document this

	// TODO(jlw) do some ProjectionExpression queries for just ids to find the starting id? Maybe do a COUNT for each day until we pass the starting point.
	s := make(data.Messages, 0, limit)

	var skipped int
	var pageErr error

	// Call this in a loop once per day in the range
	for _, day := range daysForTTL(d.ttl, d.now()) {

		// Stop querying if we've hit our limit.
		if len(s) >= limit {
			break
		}

		input := &dynamodb.QueryInput{
			TableName:              aws.String(d.table),
			ConsistentRead:         aws.Bool(d.consistent),
			Limit:                  aws.Int64(int64(limit - len(s))),
			KeyConditionExpression: aws.String("CreatedDate = :day"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":day": &dynamodb.AttributeValue{S: aws.String(day)},
			},
			ScanIndexForward: aws.Bool(false),
			// TODO(jlw) This doesn't account for the offset? Any way we can avoid the whole skip thing?
		}

		// Querying a table may require multiple calls. The sdk handles this for us
		// by calling this function once per page.
		fn := func(page *dynamodb.QueryOutput, hasNext bool) bool {
			for _, item := range page.Items {
				if skipped < start {
					skipped++
					continue
				}

				// Stop paginating if we've hit our limit.
				if len(s) == limit {
					return false
				}

				var m message
				if err := dynamodbattribute.UnmarshalMap(item, &m); err != nil {
					err = pageErr
					return false
				}
				s = append(s, *m.Msg)
			}

			// NOTE: I would like to not ask for more than I need on subsequent pages
			// but modifying the Limit after pagination has started is ignored.
			return true
		}

		if err := d.client.QueryPages(input, fn); err != nil {
			return nil, err // TODO(jlw) retry
		}
		if pageErr != nil {
			return nil, pageErr // TODO(jlw) retry
		}
	}

	// NOTE: Before we return the list we have to change their IDs from their
	// normal format to our dynamo format. This is a smell but the front end used
	// the value of msg.ID to make subsequent GET and DELETE requests.
	for i := range s {
		s[i].ID = data.MessageID(idForMsg(&s[i]))
	}

	return &s, nil
}

// Search finds messages matching the query.
func (d *Storage) Search(kind, query string, start, limit int) (*data.Messages, int, error) {
	// NOTE: I would normally return nil here for the messages but the
	// MailHog-API package ignores this error and tries to dereference the
	// pointer anyway.
	return new(data.Messages), 0, errors.New("search is not yet implemented for DynamoDB")
}
