package mhdynamo

import (
	"log" // NOTE: do not use the global logger.
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/mailhog/data"
	"github.com/pkg/errors"
)

// message is the structure we store in DynamoDB.
type message struct {
	CreatedDate string
	ID          string
	Expires     int64
	Msg         *data.Message
}

// Storage is a DynamoDB powered storage backend for MailHog. It is safe for
// concurrent use.
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
func NewStorage(client *dynamodb.DynamoDB, table string, consistent bool, ttl int) (*Storage, error) {
	if client == nil {
		return nil, errors.New("client may not be nil")
	}

	table = strings.TrimSpace(table)
	if table == "" {
		return nil, errors.New("table may not be blank")
	}

	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}

	return &Storage{
		client:     client,
		table:      table,
		consistent: consistent,
		ttl:        ttl,
		now:        time.Now,
	}, nil
}

// Store stores a message in DynamoDB and returns its storage ID.
func (s *Storage) Store(m *data.Message) (string, error) {
	msg := message{
		CreatedDate: m.Created.UTC().Format(keyFormat),
		ID:          idForMsg(m),
		Expires:     m.Created.AddDate(0, 0, s.ttl).Unix(),
		Msg:         m,
	}

	// Convert the mailhog message to a dynamodb map.
	// NOTE: This uses reflection and interfaces so it may be expensive. If
	// profiling shows a problem we can do the mapping ourselves but it's
	// complicated in creating each attribute with a type manually.
	item, err := dynamodbattribute.MarshalMap(msg)
	if err != nil {
		return "", errors.Wrap(err, "marshalling message")
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	}

	if _, err = s.client.PutItem(input); err != nil {
		return "", errors.Wrap(err, "calling PutItem")
	}

	return msg.ID, nil
}

// Load loads an individual message by storage ID.
func (s *Storage) Load(id string) (*data.Message, error) {
	day, err := dayForID(id)
	if err != nil {
		return nil, errors.Wrap(err, "decoding partition from id")
	}

	input := &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            key(day, id),
		ConsistentRead: aws.Bool(s.consistent),
	}

	output, err := s.client.GetItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "calling GetItem")
	}

	if output.Item == nil {
		return nil, errors.Errorf("dynamodb: message %q not found", id)
	}

	var m message
	if err := dynamodbattribute.UnmarshalMap(output.Item, &m); err != nil {
		return nil, errors.Wrap(err, "unmarshalling message")
	}

	// NOTE: Before we return the message we have to change their messages from
	// their normal format to the dynamo format. This is a smell but the front
	// end uses the value of msg.ID to make subsequent GET and DELETE requests.
	m.Msg.ID = data.MessageID(idForMsg(m.Msg))

	return m.Msg, nil
}

// DeleteOne deletes an individual message by storage ID.
func (s *Storage) DeleteOne(id string) error {
	day, err := dayForID(id)
	if err != nil {
		return errors.Wrap(err, "decoding partition from id")
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key:       key(day, id),
	}

	if _, err := s.client.DeleteItem(input); err != nil {
		return errors.Wrap(err, "calling DeleteItem")
	}

	return nil
}

// DeleteAll deletes all messages stored in DynamoDB.
func (s *Storage) DeleteAll() error {
	// We know all of the partition keys that should have values because they
	// have CreatedDates in the range of our TTL. Loop over those days and get the IDs
	// for that partition. Then batch delete those IDs.
	//
	// A more efficient option may be to delete the table and recreate it but we
	// can't assume to have those permissions.

	// TODO(jlw) The order we delete these does not matter so we can do it concurrently.

	// Call this in a loop once per day in the range
	for _, day := range daysForTTL(s.ttl, s.now()) {

		input := &dynamodb.QueryInput{
			TableName:              aws.String(s.table),
			ConsistentRead:         aws.Bool(s.consistent),
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
		pager := func(page *dynamodb.QueryOutput, hasNext bool) bool {
			for _, item := range page.Items {
				var m message
				if err := dynamodbattribute.UnmarshalMap(item, &m); err != nil {
					pageErr = errors.Wrap(err, "unmarshalling message")
					return false
				}
				ids = append(ids, m.ID)
			}
			return true
		}

		if err := s.client.QueryPages(input, pager); err != nil {
			return errors.Wrap(err, "calling QueryPages")
		}
		if pageErr != nil {
			return pageErr
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
						Key: key(day, id),
					},
				}
			}
			ids = ids[end:]

			input := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					s.table: batch,
				},
			}

			// Start deleting. If any UnprocessedItems come back then try again. Do
			// not retry immediately
			for i := 0; len(input.RequestItems) > 0; i++ {
				output, err := s.client.BatchWriteItem(input)
				if err != nil {
					return errors.Wrap(err, "calling BatchWriteItem")
				}
				// Replace RequestItems with any items that were unable to process.
				input.RequestItems = output.UnprocessedItems

				if i > 25 {
					return errors.New("took more than 25 tries to delete a batch")
				}
				if i > 0 {
					time.Sleep(retryInterval(i))
				}
			}
		}
	}

	return nil
}

// Count returns the number of stored messages.
func (s *Storage) Count() int {
	// NOTE: for large tables Scan (especially frequent scans) can be very slow
	// and use up all of your provisioned capacity. Consider alternatives here.
	// We could have a special partition with a single item that just stores the
	// count. Problems with that are synchronization and accounting for TTL.

	input := &dynamodb.ScanInput{
		TableName:      aws.String(s.table),
		Select:         aws.String("COUNT"),
		ConsistentRead: aws.Bool(s.consistent),
	}

	var count int64
	pager := func(scan *dynamodb.ScanOutput, hasNext bool) bool {
		count += *scan.Count
		return true
	}
	err := s.client.ScanPages(input, pager)

	// NOTE: The interface and existing implementations ignore the error on the
	// Count step. This is a bad idea. If we can't count should we panic? Log it?
	if err != nil {
		log.Printf("could not count table: %v", err)
		return 0
	}

	return int(count)
}

// List returns a list of messages sorted by date created descending (newest
// messages first). The list will include at most limit values and will begin
// at the message indexed by start.
func (s *Storage) List(start int, limit int) (*data.Messages, error) {
	// NOTE: The data scheme we use allows for consistent sort orders and
	// improved performance with Query which can ask for one partition at a time
	// rather than Scan which always read the whole table. Unfortunately the
	// mailhog interface only gives use a starting index but Dynamo does not
	// support an OFFSET parameter. The only Dynamo way to start querying at an
	// offset is to ALREADY know the ID of the item at that offset.
	//
	// I have chosen a seeking implementation which is my best-effort approach to
	// implement List according to the given interface and within the limitations
	// of Dynamo. Starting with the current day partition we ask for up to LIMIT
	// messages. As results come in we keep count of messages we have seen and
	// ignore them until we hit the START value.
	//
	// The biggest drawback of this approach is that the API does a lot of "busy
	// work" the larger the offset gets. Running List for the first several pages
	// of messages should be fast enough but the deeper into the message history
	// you go the longer each List will take.
	//
	// It may be possible to improve performance of this seeking algorithm
	// by running Query with ProjectionExpression to only retrieve IDs rather
	// than entire messages when hunting for that starting ID.
	//
	// Another alternative is to do a COUNT for each day until we pass the
	// starting point then seek for the starting point within that partition.
	// These COUNTs could be cached in a metadata partition especially for days
	// that have already past since presumably no messages from the past will
	// come in once a day is over.

	// NOTE: A more complicated alternative involves using different partitions
	// for tracking metadata such as a large array of IDs that are sorted in
	// insertion order. This would require a fair amount of complexity to keep in
	// sync especially considering TTL. Furthermore the benefits of this approach
	// might not pan out in practice without some profiling and real production
	// data to consider.

	// NOTE:

	list := make(data.Messages, 0, limit)

	var skipped int
	var pageErr error

	// Call this in a loop once per day in the range
	for _, day := range daysForTTL(s.ttl, s.now()) {

		// Stop querying if we've hit our limit.
		if len(list) >= limit {
			break
		}

		input := &dynamodb.QueryInput{
			TableName:              aws.String(s.table),
			ConsistentRead:         aws.Bool(s.consistent),
			Limit:                  aws.Int64(int64(limit - len(list))),
			KeyConditionExpression: aws.String("CreatedDate = :day"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":day": &dynamodb.AttributeValue{S: aws.String(day)},
			},
			ScanIndexForward: aws.Bool(false),
		}

		// Querying a table may require multiple calls. The SDK handles this for us
		// by calling this function once per page.
		pager := func(page *dynamodb.QueryOutput, hasNext bool) bool {
			for _, item := range page.Items {
				if skipped < start {
					skipped++
					continue
				}

				// Stop paginating if we've hit our limit.
				if len(list) == limit {
					return false
				}

				var m message
				if err := dynamodbattribute.UnmarshalMap(item, &m); err != nil {
					pageErr = errors.Wrap(err, "unmarshalling item")
					return false
				}
				list = append(list, *m.Msg)
			}

			// NOTE: I would like to not ask for more than I need on subsequent pages
			// but modifying the Limit after pagination has started is ignored.
			return true
		}

		if err := s.client.QueryPages(input, pager); err != nil {
			return nil, errors.Wrap(err, "calling QueryPages")
		}
		if pageErr != nil {
			return nil, pageErr
		}
	}

	// NOTE: Before we return the list we have to change their IDs from their
	// normal format to our dynamo format. This is a smell but the front end uses
	// the value of msg.ID to make subsequent GET and DELETE requests.
	for i := range list {
		list[i].ID = data.MessageID(idForMsg(&list[i]))
	}

	return &list, nil
}

// Search finds messages matching the query.
func (s *Storage) Search(kind, query string, start, limit int) (*data.Messages, int, error) {
	// NOTE: I would normally return nil here for the messages but the
	// MailHog-API package ignores this error and tries to dereference the
	// pointer anyway.
	return new(data.Messages), 0, errors.New("search is not yet implemented for DynamoDB")
}
