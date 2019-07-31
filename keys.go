package mhdynamo

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mailhog/data"
)

// keyFormat defines how message creation times are formatted to day partition keys.
const keyFormat = "2006-01-02"

func key(day, id string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"CreatedDate": &dynamodb.AttributeValue{S: aws.String(day)},
		"ID":          &dynamodb.AttributeValue{S: aws.String(id)},
	}
}

// idForMsg knows how to generate a string that encodes the dynamo ID for a
// message. The ID has two parts: the ID of the message and the created date.
// It is stored with the created date first as a unix timestamp in nanoseconds
// because that representation is a fixed width and it's sortable. A pipe
// character | separates the components for human readability.
func idForMsg(m *data.Message) string {
	var sb strings.Builder
	sb.Grow(20 + len(m.ID)) // preallocate enough space. 19 for the timesamp, 1 for the |, plus the id
	sb.WriteString(strconv.Itoa(int(m.Created.UnixNano())))
	sb.WriteRune('|')
	sb.WriteString(string(m.ID))
	return sb.String()
}

// dayForID decodes the partition key (DayKey) from a storage ID. It will
// be a string in YYYY-MM-DD format.
func dayForID(id string) (string, error) {
	if len(id) < 21 {
		return "", errors.New("id is too short")
	}

	nano, err := strconv.Atoi(id[:19])
	if err != nil {
		return "", err
	}
	created := time.Unix(0, int64(nano))

	return created.UTC().Format(keyFormat), nil
}

// daysForTTL gives the range of days that should be used as partition
// keys for querying items inside the TTL range.
func daysForTTL(ttl int, now time.Time) []string {
	days := make([]string, ttl)

	now = now.UTC()

	for i := 0; i < ttl; i++ {
		days[i] = now.Format("2006-01-02")
		now = now.AddDate(0, 0, -1)
	}

	return days
}
