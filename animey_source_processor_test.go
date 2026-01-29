//go:build source_processor

package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"net/url"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-lambda-go/events"
)

func TestHandler(t *testing.T) {
	if os.Getenv("DYNAMODB_IDEMPOTENCY_TABLE") == "" || os.Getenv("S3_OUTPUT_BUCKET") == "" {
		t.Fatal("missing S3_IDEMPOTENCY_TABLE or S3_OUTPUT_BUCKET env vars")
	}

	_, err := Handler(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{
				MessageId: "02619172-bcef-4ff8-984a-bac9e8381b6f",
				Body:      `{"episode_id":"102296","episode_title":"Idol","episode_number":"11","series_id":"18330","series_name":"My Star","season_number":"1","link":"https://megacloud.blog/embed-2/v3/e-1/1wRUFJwLATeD?k=1","server":4}`,
			},
		},
	})

	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
}

func TestExtractAccessKeyFallback(t *testing.T) {
	body, err := os.ReadFile("test.html")
	if err != nil {
		t.Fatalf("read test.html: %v", err)
	}

	key, err := extractAccessKey(string(body))
	if err != nil {
		t.Fatalf("extractAccessKey error: %v", err)
	}

	expected := "Tvn3hGj1HhXmqvu15iBAnNL7ktAkSkgOPq9cNl52lAQgGD6l"
	if key != expected {
		t.Fatalf("unexpected access key: got %q want %q", key, expected)
	}
}

func TestParseEpisodeMessage(t *testing.T) {
	record := events.SQSMessage{
		Body: `{"episode_id":"100244","episode_title":"Third Option","episode_number":"2","series_id":"18330","series_name":"My Star","season_number":"1","link":"https://example.com","server":4}`,
	}

	payload, err := parseEpisodeMessage(record)
	if err != nil {
		t.Fatalf("parseEpisodeMessage error: %v", err)
	}

	if payload.EpisodeID != "100244" {
		t.Fatalf("unexpected episode_id: %q", payload.EpisodeID)
	}
	if payload.SeriesName != "My Star" {
		t.Fatalf("unexpected series_name: %q", payload.SeriesName)
	}
}

func TestParseEpisodeMessageMissingFields(t *testing.T) {
	record := events.SQSMessage{
		Body: `{"episode_id":"100244","episode_title":"","episode_number":"2","series_name":"My Star","season_number":"1","link":"https://example.com"}`,
	}

	if _, err := parseEpisodeMessage(record); err == nil {
		t.Fatal("expected error for missing episode_title")
	}
}

func TestBuildEpisodeObjectKey(t *testing.T) {
	payload := episodeMessage{
		EpisodeTitle:  "Third Option",
		EpisodeNumber: "2",
		SeriesName:    "My Star",
		SeasonNumber:  "1",
	}

	key, err := buildEpisodeObjectKey(payload, "output/prefix")
	if err != nil {
		t.Fatalf("buildEpisodeObjectKey error: %v", err)
	}

	expected := "output/prefix/My Star/Season 1/My Star S01E02 Third Option.ts"
	if key != expected {
		t.Fatalf("unexpected key: got %q want %q", key, expected)
	}
}

func TestGetMegacloudPlayerAttributes(t *testing.T) {
	html := `
		<div id="megacloud-player" data-id="123" data-realid="456" data-mediaid="789"></div>
	`
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("parse html: %v", err)
	}

	attrs, err := getMegacloudPlayerAttributes(doc)
	if err != nil {
		t.Fatalf("getMegacloudPlayerAttributes error: %v", err)
	}
	if attrs.DataID != "123" || attrs.RealID != int64(456) || attrs.MediaID != int64(789) {
		t.Fatalf("unexpected attributes: %+v", attrs)
	}
}

func TestParseNumericAttribute(t *testing.T) {
	val, err := parseNumericAttribute("data-id", "00042")
	if err != nil {
		t.Fatalf("parseNumericAttribute error: %v", err)
	}
	if val != 42 {
		t.Fatalf("unexpected value: %d", val)
	}

	if _, err := parseNumericAttribute("data-id", "12a"); err == nil {
		t.Fatal("expected error for non-numeric attribute")
	}
}

func TestSelectHighestResolutionPlaylist(t *testing.T) {
	master := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360",
		"low/playlist.m3u8",
		"#EXT-X-STREAM-INF:BANDWIDTH=1600000,RESOLUTION=1280x720",
		"mid/playlist.m3u8",
		"#EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1920x1080",
		"hi/playlist.m3u8",
	}, "\n")

	baseURL, err := url.Parse("https://example.com/master.m3u8")
	if err != nil {
		t.Fatalf("parse base url: %v", err)
	}

	selected, err := selectHighestResolutionPlaylist(master, baseURL)
	if err != nil {
		t.Fatalf("selectHighestResolutionPlaylist error: %v", err)
	}
	if selected != "https://example.com/hi/playlist.m3u8" {
		t.Fatalf("unexpected playlist url: %q", selected)
	}
}

func TestParseResolution(t *testing.T) {
	line := `#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=1280x720`
	if val := parseResolution(line); val != 1280*720 {
		t.Fatalf("unexpected resolution value: %d", val)
	}
	if val := parseResolution("#EXT-X-STREAM-INF:BANDWIDTH=800000"); val != 0 {
		t.Fatalf("expected zero resolution, got: %d", val)
	}
}

func TestResolvePlaylistURL(t *testing.T) {
	baseURL, err := url.Parse("https://cdn.example.com/path/master.m3u8")
	if err != nil {
		t.Fatalf("parse base url: %v", err)
	}
	resolved, err := resolvePlaylistURL(baseURL, "sub/variant.m3u8")
	if err != nil {
		t.Fatalf("resolvePlaylistURL error: %v", err)
	}
	if resolved != "https://cdn.example.com/path/sub/variant.m3u8" {
		t.Fatalf("unexpected resolved url: %q", resolved)
	}
}

func TestExtractSegmentURLs(t *testing.T) {
	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-TARGETDURATION:6",
		"#EXTINF:6.0,",
		"seg-1.ts",
		"#EXTINF:6.0,",
		"seg-2.ts",
	}, "\n")
	playlistURL, err := url.Parse("https://media.example.com/path/playlist.m3u8")
	if err != nil {
		t.Fatalf("parse playlist url: %v", err)
	}
	segments, err := extractSegmentURLs(playlist, playlistURL)
	if err != nil {
		t.Fatalf("extractSegmentURLs error: %v", err)
	}
	if len(segments) != 2 {
		t.Fatalf("unexpected segment count: %d", len(segments))
	}
	if segments[0] != "https://media.example.com/path/seg-1.ts" {
		t.Fatalf("unexpected segment url: %q", segments[0])
	}
}

func TestFirstHlsSourceURL(t *testing.T) {
	items := []sourceItem{
		{File: "https://example.com/video.mp4", Type: "mp4"},
		{File: "https://example.com/stream.m3u8", Type: "hls"},
	}
	hlsURL, err := firstHlsSourceURL(items)
	if err != nil {
		t.Fatalf("firstHlsSourceURL error: %v", err)
	}
	if hlsURL != "https://example.com/stream.m3u8" {
		t.Fatalf("unexpected hls url: %q", hlsURL)
	}
}
