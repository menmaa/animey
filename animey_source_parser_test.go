//go:build source_parser

package main

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHandler(t *testing.T) {
	result, err := Handler(context.Background(), Event{
		URL:    "https://hianime.to/watch/my-star-18330",
		Name:   "Oshi No Ko",
		Season: "2",
	})

	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	s, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("result is not a valid JSON object: %v", err)
	}

	t.Logf("%s", s)
}
