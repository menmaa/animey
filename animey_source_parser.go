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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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

type SourceResult struct {
	EpisodeID     string `json:"episode_id"`
	EpisodeTitle  string `json:"episode_title"`
	EpisodeNumber string `json:"episode_number"`
	SeriesID      string `json:"series_id"`
	SeriesName    string `json:"series_name"`
	SeasonNumber  string `json:"season_number"`
	URL           string `json:"url"`
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
	sqsClient  *sqs.Client
	queueURL   string
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

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		baseLogger.Error("Failed to load AWS config.", zap.Error(err))
		return
	}

	sqsClient = sqs.NewFromConfig(cfg)
}

func Handler(ctx context.Context, event Event) ([]SourceResult, error) {
	// queueURL = strings.TrimSpace(os.Getenv("DESTINATION_SQS_QUEUE"))
	// if queueURL == "" {
	// 	baseLogger.Error("Destination SQS queue not set.", zap.String("env_var", "DESTINATION_SQS_QUEUE"))
	// 	return nil, errors.New("destination sqs queue not set")
	// }

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

	results := make([]SourceResult, 0)
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

		url, err := fetchSourceURL(ctx, client, logger, serverIDs)
		if err != nil {
			return nil, err
		}

		results = append(results, SourceResult{
			EpisodeID:     item.DataID,
			EpisodeTitle:  item.Title,
			EpisodeNumber: item.Number,
			SeriesID:      seriesID,
			SeriesName:    seriesName,
			SeasonNumber:  seasonNumber,
			URL:           url,
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
	doc.Find(".server-item").Each(func(_ int, s *goquery.Selection) {
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

func fetchSourceURL(ctx context.Context, client *http.Client, logger *zap.Logger, serverIDs []string) (string, error) {
	for idx, serverID := range serverIDs {
		url := fmt.Sprintf(sourcesURLQuery, serverID)
		logger.Debug("Fetching episode source JSON.", zap.String("url", url), zap.String("server_id", serverID))

		var resp apiSourceResponse
		if err := fetchJSON(ctx, client, url, &resp); err != nil {
			logger.Error("Episode source fetch failed.",
				zap.String("server_id", serverID),
				zap.Error(err),
			)
		} else if strings.TrimSpace(resp.Link) == "" {
			logger.Error("Episode source missing link.", zap.String("server_id", serverID))
		} else {
			return resp.Link, nil
		}

		if idx < len(serverIDs)-1 {
			logger.Debug("Sleeping before next source attempt.", zap.Duration("sleep", 6*time.Second))
			time.Sleep(6 * time.Second)
		}
	}

	logger.Error("All episode source attempts failed.")
	return "", errors.New("all episode source attempts failed")
}

func sendSourceResultToSQS(ctx context.Context, client *sqs.Client, queueURL string, result SourceResult, logger *zap.Logger) error {
	if client == nil {
		return errors.New("sqs client is required")
	}

	if strings.TrimSpace(queueURL) == "" {
		return errors.New("queue url is required")
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	logger.Debug("Sending SQS message.",
		zap.String("queue_url", queueURL),
		zap.String("episode_id", result.EpisodeID),
	)

	payload, err := json.Marshal(result)
	if err != nil {
		logger.Error("Failed to marshal SQS payload.", zap.Error(err))
		return err
	}

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(payload)),
	})
	if err != nil {
		logger.Error("Failed to send SQS message.", zap.Error(err))
		return err
	}

	return nil
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
