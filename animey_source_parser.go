//go:build source_parser

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-lambda-go/lambda"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	userAgentHeader   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
	episodeListURL    = "https://hianime.to/ajax/v2/episode/list/%s"
	episodeServersURL = "https://hianime.to/ajax/v2/episode/servers?episodeId=%s"
	sourcesURLQuery   = "https://hianime.to/ajax/v2/episode/sources?id=%s"
)

type Event struct {
	Name   string `json:"name"`
	Season string `json:"season"`
	URL    string `json:"url"`
}

type EpisodeResult struct {
	EpisodeID     string   `json:"episode_id"`
	EpisodeTitle  string   `json:"episode_title"`
	EpisodeNumber string   `json:"episode_number"`
	SeriesID      string   `json:"series_id"`
	SeriesName    string   `json:"series_name"`
	SeasonNumber  string   `json:"season_number"`
	Servers       []string `json:"servers"`
}

type listItem struct {
	DataID string
	Title  string
	Number string
}

type apiHTMLResponse struct {
	Status bool   `json:"status"`
	HTML   string `json:"html"`
}

type apiSourceResponse struct {
	Link   string `json:"link"`
	Server int32  `json:"server"`
}

var (
	baseLogger *zap.Logger
)

func init() {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.DisableCaller = true
	loggerConfig.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendInt64(t.Unix())
	}

	var err error
	baseLogger, err = loggerConfig.Build()
	if err != nil {
		baseLogger = zap.NewNop()
	}
}

func Handler(ctx context.Context, event Event) ([]EpisodeResult, error) {
	if event.URL == "" {
		return nil, errors.New("url is required")
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	seasonNumber := "1"
	seriesID, seriesName, err := fetchSeriesData(ctx, client, baseLogger, event.URL)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(event.Name) != "" {
		seriesName = event.Name
	}

	if strings.TrimSpace(event.Season) != "" {
		seasonNumber = event.Season
	}

	logger := baseLogger.With(zap.String("series_id", seriesID))

	items, err := fetchEpisodes(ctx, client, logger, seriesID)
	if err != nil {
		return nil, err
	}

	results := make([]EpisodeResult, 0)
	for _, item := range items {
		logger.Info("Processing episode.",
			zap.String("episode_id", item.DataID),
			zap.String("episode_title", item.Title),
			zap.String("episode_number", item.Number),
		)

		serverIDs, err := fetchEpisodeServers(ctx, client, logger, item.DataID)
		if err != nil {
			return nil, err
		}

		results = append(results, EpisodeResult{
			EpisodeID:     item.DataID,
			EpisodeTitle:  item.Title,
			EpisodeNumber: item.Number,
			SeriesID:      seriesID,
			SeriesName:    seriesName,
			SeasonNumber:  seasonNumber,
			Servers:       serverIDs,
		})
	}

	return results, nil
}

func fetchSeriesData(ctx context.Context, client *http.Client, logger *zap.Logger, url string) (string, string, error) {
	logger.Info("Fetching base HTML.", zap.String("request_url", url))
	doc, err := fetchHTMLDoc(ctx, client, url)
	if err != nil {
		logger.Error("Failed to fetch base HTML.", zap.Error(err))
		return "", "", err
	}

	seriesID, seriesName, err := getSeriesData(doc)
	if err != nil {
		logger.Error("Failed to parse series sync data.", zap.Error(err))
		return "", "", err
	}

	return seriesID, seriesName, nil
}

func fetchEpisodes(ctx context.Context, client *http.Client, logger *zap.Logger, dataID string) ([]listItem, error) {
	url := fmt.Sprintf(episodeListURL, dataID)
	logger.Info("Fetching episode list.")

	var resp apiHTMLResponse
	if err := fetchJSON(ctx, client, url, &resp); err != nil {
		logger.Error("Episode list request failed.", zap.Error(err))
		return nil, err
	}

	if !resp.Status {
		logger.Error("Episode list API returned status false.")
		return nil, errors.New("list api returned status false")
	}
	if strings.TrimSpace(resp.HTML) == "" {
		logger.Error("Episode list API returned empty HTML.")
		return nil, errors.New("list api returned empty html")
	}

	doc, err := htmlDocFromString(resp.HTML)
	if err != nil {
		logger.Error("Episode list HTML parsing failed.", zap.Error(err))
		return nil, err
	}

	episodes := make([]listItem, 0)
	doc.Find("a").Each(func(_ int, s *goquery.Selection) {
		id, ok := s.Attr("data-id")
		if !ok || strings.TrimSpace(id) == "" {
			return
		}

		title, _ := s.Attr("title")
		number, _ := s.Attr("data-number")
		episodes = append(episodes, listItem{
			DataID: id,
			Title:  title,
			Number: number,
		})
	})

	if len(episodes) == 0 {
		logger.Error("No list items found in HTML.")
		return nil, errors.New("no list items found in html")
	}

	return episodes, nil
}

func fetchEpisodeServers(ctx context.Context, client *http.Client, logger *zap.Logger, episodeID string) ([]string, error) {
	url := fmt.Sprintf(episodeServersURL, episodeID)
	logger.Debug("Fetching episode server list.", zap.String("url", url), zap.String("episode_id", episodeID))

	var resp apiHTMLResponse
	if err := fetchJSON(ctx, client, url, &resp); err != nil {
		logger.Error("Episode server list request failed.", zap.String("episode_id", episodeID), zap.Error(err))
		return nil, err
	}

	if !resp.Status {
		logger.Error("Episode server list API returned status false.", zap.String("episode_id", episodeID))
		return nil, errors.New("episode server list api returned status false")
	}

	if strings.TrimSpace(resp.HTML) == "" {
		logger.Error("Episode server list API returned empty HTML.", zap.String("episode_id", episodeID))
		return nil, errors.New("episode server list api returned empty html")
	}

	doc, err := htmlDocFromString(resp.HTML)
	if err != nil {
		logger.Error("Episode server list HTML parsing failed.", zap.String("episode_id", episodeID), zap.Error(err))
		return nil, err
	}

	ids := make([]string, 0)
	doc.Find(".server-item[data-type=\"sub\"]").Each(func(_ int, s *goquery.Selection) {
		id, ok := s.Attr("data-id")
		if !ok || strings.TrimSpace(id) == "" {
			return
		}
		ids = append(ids, id)
	})

	if len(ids) == 0 {
		logger.Error("No episode server IDs found.", zap.String("episode_id", episodeID))
		return nil, errors.New("no episode server ids found")
	}

	return ids, nil
}

func fetchHTMLDoc(ctx context.Context, client *http.Client, url string) (*goquery.Document, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgentHeader)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return goquery.NewDocumentFromReader(resp.Body)
}

func fetchJSON(ctx context.Context, client *http.Client, url string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgentHeader)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	return decoder.Decode(target)
}

func htmlDocFromString(html string) (*goquery.Document, error) {
	return goquery.NewDocumentFromReader(strings.NewReader(html))
}

func getSeriesData(doc *goquery.Document) (string, string, error) {
	selection := doc.Find("#syncData").First()
	if selection.Length() == 0 {
		return "", "", errors.New("#syncData element not found")
	}

	payload := strings.TrimSpace(selection.Text())
	if payload == "" {
		return "", "", errors.New("syncData payload is empty")
	}

	var syncData struct {
		Name    string `json:"name"`
		AnimeID string `json:"anime_id"`
	}
	if err := json.Unmarshal([]byte(payload), &syncData); err != nil {
		return "", "", err
	}

	if strings.TrimSpace(syncData.AnimeID) == "" {
		return "", "", errors.New(`"anime_id" missing from syncData`)
	}
	if strings.TrimSpace(syncData.Name) == "" {
		return "", "", errors.New(`"name" missing from syncData`)
	}

	return syncData.AnimeID, syncData.Name, nil
}

func main() {
	defer baseLogger.Sync()
	lambda.Start(Handler)
}
