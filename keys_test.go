package mhdynamo

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/mailhog/data"
)

// TestIDFromMsg exercises the function for encoding messages into storage ids.
func TestIDFromMsg(t *testing.T) {
	msg := data.Message{
		ID:      "id",
		Created: time.Unix(1234567890, 1),
	}
	want := "1234567890000000001|id"
	got := idForMsg(&msg)
	if got != want {
		t.Errorf("idForMsg(%+v) = %q, want %q", msg, got, want)
	}
}

// TestDayForID calculates the Partition Key (Day) for an ID.
func TestDayForID(t *testing.T) {
	id := "1234567890000000001|id"
	want := "2009-02-13"

	got, err := dayForID(id)
	if err != nil {
		t.Fatalf("dayForID(%q) should not err, got %v", id, err)
	}
	if got != want {
		t.Errorf("dayForID(%q) = day %q, want %q", id, got, want)
	}
}

func TestDaysForTTL(t *testing.T) {
	// With 7 days ttl, a query ran on 2019-09-04 should include messages from
	// that date and the 6 days prior.
	ttl := 7
	now := time.Date(2019, time.September, 04, 12, 00, 00, 00, time.UTC)
	want := []string{
		"2019-09-04",
		"2019-09-03",
		"2019-09-02",
		"2019-09-01",
		"2019-08-31",
		"2019-08-30",
		"2019-08-29",
	}

	got := daysForTTL(ttl, now)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("daysForTTL(%d, %v) did not match:\n%s", ttl, now, diff)
	}
}
