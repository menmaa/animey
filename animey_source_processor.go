//go:build source_processor

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	userAgentHeader = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
	sourcesURLQuery = "https://hianime.to/ajax/v2/episode/sources?id=%s"
)

var (
	accessKeyRegex = regexp.MustCompile(
		`(?:<meta\s+name="_gg_fb"\s+content="([^"]+)"|` +
			`window\._xy_ws\s*=\s*"([^"]+)"|` +
			`<script[^>]*\snonce="([^"]+)"|` +
			`data-dpi="([^"]+)"|` +
			`<!--\s*_is_th:([^\s-]+))`,
	)
	accessKeyFallbackRegex = regexp.MustCompile(
		`window\._lk_db\s*=\s*\{[^}]*x:\s*"([^"]+)"[^}]*y:\s*"([^"]+)"[^}]*z:\s*"([^"]+)"[^}]*\}`,
	)
	accessKeyValidRegex  = regexp.MustCompile(`^[A-Za-z0-9]{48}$`)
	numericOnlyRegex     = regexp.MustCompile(`^\d+$`)
	logger               *zap.Logger
	s3Client             *s3.Client
	s3OutputBucket       = strings.TrimSpace(os.Getenv("S3_OUTPUT_BUCKET"))
	s3OutputPrefix       = strings.Trim(strings.TrimSpace(os.Getenv("S3_OUTPUT_PREFIX")), "/")
	s3OutputStorageClass = strings.TrimSpace(os.Getenv("S3_OUTPUT_STORAGE_CLASS"))
)

type EpisodeObject struct {
	EpisodeID     string   `json:"episode_id"`
	EpisodeTitle  string   `json:"episode_title"`
	EpisodeNumber string   `json:"episode_number"`
	SeriesID      string   `json:"series_id"`
	SeriesName    string   `json:"series_name"`
	SeasonNumber  string   `json:"season_number"`
	Servers       []string `json:"servers"`
}

type MegacloudPlayerAttributes struct {
	DataID  string
	RealID  int64
	MediaID int64
}

type apiSourceResponse struct {
	Link   string `json:"link"`
	Server int32  `json:"server"`
}

type sourcesResponse struct {
	Sources []sourceItem `json:"sources"`
	Tracks  []trackItem  `json:"tracks"`
}

type sourceItem struct {
	File string `json:"file"`
	Type string `json:"type"`
}

type trackItem struct {
	File    string `json:"file"`
	Label   string `json:"label"`
	Kind    string `json:"kind"`
	Default bool   `json:"default"`
}

func init() {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	loggerConfig.DisableCaller = true
	loggerConfig.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendInt64(t.Unix())
	}

	var err error
	logger, err = loggerConfig.Build()
	if err != nil {
		logger = zap.NewNop()
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("Failed to load AWS config.", zap.Error(err))
		return
	}
	s3Client = s3.NewFromConfig(cfg)

	if s3OutputBucket == "" {
		logger.Error("S3_OUTPUT_BUCKET not set.")
		return
	}

	if s3OutputStorageClass == "" {
		s3OutputStorageClass = "STANDARD"
	}
}

func main() {
	defer logger.Sync()

	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	payload, err := parseEpisodeMessage(os.Getenv("INPUT_RECORD"))
	if err != nil {
		logger.Error("Failed to parse input payload.", zap.Error(err))
		return err
	}

	mediaObjectKey, subtitlesObjectKey, err := buildEpisodeObjectKey(payload)
	if err != nil {
		logger.Error("Failed to build output key.", zap.Error(err))
		return err
	}

	for _, server := range payload.Servers {
		logger.Debug("Processing episode media.", zap.String("server_url", server))
		if err := processMedia(context.Background(), client, server, mediaObjectKey, subtitlesObjectKey); err != nil {
			logger.Warn("Processing media failed. Trying next available server...", zap.Error(err))
			continue
		}

		return nil
	}

	logger.Error("No available media servers left.")
	return errors.New("no available media servers left")
}

func processMedia(ctx context.Context, client *http.Client, serverID, mediaObjectKey, subtitlesObjectKey string) error {
	logger.Info("Processing episode download.", zap.String("server_id", serverID))
	if strings.TrimSpace(serverID) == "" {
		return errors.New("server_id is required")
	}

	serverUrl, err := fetchServerURL(ctx, client, serverID)
	if err != nil {
		logger.Error("Fetching Server URL failed.", zap.Error(err))
		return err
	}

	logger.Debug("Fetching HTML document.")
	html, doc, err := fetchHTML(ctx, client, serverUrl)
	if err != nil {
		logger.Error("Fetching HTML failed.", zap.Error(err))
		return err
	}

	logger.Debug("Extracting megacloud player attributes.")
	attributes, err := getMegacloudPlayerAttributes(doc)
	if err != nil {
		logger.Error("Extracting megacloud attributes failed.", zap.Error(err))
		return err
	}

	logger.Debug("Extracting access key.")
	accessKey, err := extractAccessKey(html)
	if err != nil {
		logger.Error("Extracting access key failed.", zap.Error(err))
		return err
	}

	logger.Debug("Building sources URL.")
	sourcesURL, err := buildSourcesURL(serverUrl, attributes.DataID, accessKey)
	if err != nil {
		logger.Error("Building sources URL failed.", zap.Error(err))
		return err
	}

	logger.Debug("Fetching sources JSON.", zap.String("sources_url", sourcesURL))
	var sources sourcesResponse
	if err := fetchJSON(ctx, client, sourcesURL, &sources); err != nil {
		logger.Error("Fetching sources JSON failed.", zap.Error(err))
		return err
	}

	if len(sources.Sources) == 0 {
		logger.Error("Sources list empty.")
		return errors.New("sources list is empty")
	}

	logger.Debug("Selecting first HLS source.")
	hlsURL, err := firstHlsSourceURL(sources.Sources)
	if err != nil {
		logger.Error("Selecting HLS source failed.", zap.Error(err))
		return err
	}

	logger.Debug("Selecting default subtitle track.")
	subtitleTrackURL, err := defaultSubtitlesSourceURL(sources.Tracks)
	if err != nil {
		logger.Warn("No default subtitle track found.")
	}

	logger.Debug("Resolving origin and referer.")
	origin, referer, err := originAndReferer(serverUrl)
	if err != nil {
		logger.Error("Resolving origin/referer failed.", zap.Error(err))
		return err
	}

	logger.Debug("Fetching master playlist.", zap.String("hls_url", hlsURL))
	masterPlaylist, masterURL, err := fetchText(ctx, client, hlsURL, origin, referer)
	if err != nil {
		logger.Error("Fetching master playlist failed.", zap.Error(err))
		return err
	}

	logger.Debug("Selecting highest resolution variant.")
	playlistURL, err := selectHighestResolutionPlaylist(masterPlaylist, masterURL)
	if err != nil {
		logger.Error("Selecting highest resolution variant failed.", zap.Error(err))
		return err
	}

	logger.Info("Downloading episode...", zap.String("playlist_url", playlistURL))
	mediaOutputPath, err := downloadEpisode(ctx, playlistURL, mediaObjectKey, origin, referer)
	if err != nil {
		logger.Error("Downloading episode failed.", zap.Error(err))
		return err
	}

	var subtitlesOutputPath string
	if subtitleTrackURL != "" {
		logger.Debug("Downloading subtitles...", zap.String("subtitle_track", subtitleTrackURL))
		subtitlesOutputPath, err = downloadSubtitles(ctx, subtitleTrackURL, subtitlesObjectKey, origin, referer)
		if err != nil {
			logger.Error("Downloading episode failed.", zap.Error(err))
			return err
		}
	}

	logger.Info("Episode processed.", zap.String("s3_media_output_path", mediaOutputPath), zap.String("s3_subtitles_output_path", subtitlesOutputPath))
	return nil
}

func fetchServerURL(ctx context.Context, client *http.Client, serverID string) (string, error) {
	url := fmt.Sprintf(sourcesURLQuery, serverID)
	logger.Debug("Fetching episode server JSON.", zap.String("url", url), zap.String("server_id", serverID))

	var resp apiSourceResponse
	if err := fetchJSON(ctx, client, url, &resp); err != nil {
		logger.Error("Episode server fetch failed.",
			zap.String("server_id", serverID),
			zap.Error(err),
		)
		return "", errors.New("failed to retrieve server url for server id.")
	}
	if strings.TrimSpace(resp.Link) == "" {
		logger.Error("Episode server missing link.", zap.String("server_id", serverID))
		return "", errors.New("failed to retrieve server url for server id.")
	}

	logger.Debug("Fetched episode server URL.", zap.String("server_url", resp.Link))
	return resp.Link, nil

}

func fetchHTML(ctx context.Context, client *http.Client, requestURL string) (string, *goquery.Document, error) {
	logger.Debug("HTTP GET HTML.", zap.String("url", requestURL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", userAgentHeader)
	req.Header.Set("Referer", "https://hianime.to/")

	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	logger.Debug("HTML response received.", zap.Int("status", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", nil, err
	}

	return string(body), doc, nil
}

func fetchJSON(ctx context.Context, client *http.Client, requestURL string, target any) error {
	logger.Debug("HTTP GET JSON.", zap.String("url", requestURL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgentHeader)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	logger.Debug("JSON response received.", zap.Int("status", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	return decoder.Decode(target)
}

func parseEpisodeMessage(record string) (EpisodeObject, error) {
	body := strings.TrimSpace(record)
	if body == "" {
		return EpisodeObject{}, errors.New("record body is empty")
	}

	var payload EpisodeObject
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return EpisodeObject{}, err
	}
	if strings.TrimSpace(payload.SeriesName) == "" {
		return EpisodeObject{}, errors.New("payload series_name is required")
	}
	if strings.TrimSpace(payload.SeasonNumber) == "" {
		return EpisodeObject{}, errors.New("payload season_number is required")
	}
	if strings.TrimSpace(payload.EpisodeNumber) == "" {
		return EpisodeObject{}, errors.New("payload episode_number is required")
	}
	if strings.TrimSpace(payload.EpisodeTitle) == "" {
		return EpisodeObject{}, errors.New("payload episode_title is required")
	}
	if len(payload.Servers) == 0 {
		return EpisodeObject{}, errors.New("at least 1 server url is required")
	}

	return payload, nil
}

func buildEpisodeObjectKey(payload EpisodeObject) (string, string, error) {
	seasonValue, err := strconv.Atoi(strings.TrimSpace(payload.SeasonNumber))
	if err != nil {
		return "", "", fmt.Errorf("season_number must be numeric: %w", err)
	}
	episodeValue, err := strconv.Atoi(strings.TrimSpace(payload.EpisodeNumber))
	if err != nil {
		return "", "", fmt.Errorf("episode_number must be numeric: %w", err)
	}

	seriesName := strings.TrimSpace(payload.SeriesName)
	episodeTitle := strings.TrimSpace(payload.EpisodeTitle)
	if seriesName == "" || episodeTitle == "" {
		return "", "", errors.New("series_name and episode_title are required")
	}

	seasonNumber := strconv.Itoa(seasonValue)
	seasonPadded := fmt.Sprintf("%02d", seasonValue)
	episodePadded := fmt.Sprintf("%02d", episodeValue)

	mediaKey := fmt.Sprintf(
		"%s/Season %s/%s S%sE%s %s.mp4",
		seriesName,
		seasonNumber,
		seriesName,
		seasonPadded,
		episodePadded,
		episodeTitle,
	)

	if strings.TrimSpace(s3OutputPrefix) != "" {
		mediaKey = path.Join(strings.Trim(s3OutputPrefix, "/"), mediaKey)
	}
	subtitleKey := strings.Replace(mediaKey, ".mp4", ".srt", 1)

	return mediaKey, subtitleKey, nil
}

func fetchText(ctx context.Context, client *http.Client, requestURL, origin, referer string) (string, *url.URL, error) {
	logger.Debug("HTTP GET text.", zap.String("url", requestURL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", userAgentHeader)
	req.Header.Set("Origin", origin)
	req.Header.Set("Referer", referer)

	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	logger.Debug("Text response received.", zap.Int("status", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}

	parsedURL, err := url.Parse(requestURL)
	if err != nil {
		return "", nil, err
	}

	return string(body), parsedURL, nil
}

func getMegacloudPlayerAttributes(doc *goquery.Document) (MegacloudPlayerAttributes, error) {
	logger.Debug("Parsing #megacloud-player element.")
	selection := doc.Find("#megacloud-player").First()
	if selection.Length() == 0 {
		return MegacloudPlayerAttributes{}, errors.New("#megacloud-player element not found")
	}

	dataID, ok := selection.Attr("data-id")
	if !ok || strings.TrimSpace(dataID) == "" {
		return MegacloudPlayerAttributes{}, errors.New(`attribute "data-id" not found on #megacloud-player`)
	}

	realID, ok := selection.Attr("data-realid")
	if !ok || strings.TrimSpace(realID) == "" {
		return MegacloudPlayerAttributes{}, errors.New(`attribute "data-realid" not found on #megacloud-player`)
	}

	mediaID, ok := selection.Attr("data-mediaid")
	if !ok || strings.TrimSpace(mediaID) == "" {
		return MegacloudPlayerAttributes{}, errors.New(`attribute "data-mediaid" not found on #megacloud-player`)
	}

	realIDValue, err := parseNumericAttribute("data-realid", realID)
	if err != nil {
		return MegacloudPlayerAttributes{}, err
	}
	mediaIDValue, err := parseNumericAttribute("data-mediaid", mediaID)
	if err != nil {
		return MegacloudPlayerAttributes{}, err
	}

	return MegacloudPlayerAttributes{
		DataID:  dataID,
		RealID:  realIDValue,
		MediaID: mediaIDValue,
	}, nil
}

func parseNumericAttribute(attrName, value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("%s attribute is empty", attrName)
	}
	if !numericOnlyRegex.MatchString(trimmed) {
		return 0, fmt.Errorf("%s attribute must be numeric", attrName)
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s attribute parse failed: %w", attrName, err)
	}
	return parsed, nil
}

func extractAccessKey(html string) (string, error) {
	logger.Debug("Searching for access key.")
	accessKey := firstRegexMatch(accessKeyRegex, html)
	if accessKey == "" {
		matches := accessKeyFallbackRegex.FindStringSubmatch(html)
		if len(matches) >= 4 {
			accessKey = strings.TrimSpace(matches[1]) + strings.TrimSpace(matches[2]) + strings.TrimSpace(matches[3])
		}
	}
	if accessKey == "" {
		return "", errors.New("access key not found")
	}
	if !accessKeyValidRegex.MatchString(accessKey) {
		logger.Error("Access key doesn't match the expected format.", zap.String("access_key", accessKey), zap.String("html", html))
		return "", fmt.Errorf("access key doesn't match the expected format")
	}
	logger.Debug("Access key found.", zap.String("access_key", accessKey))
	return accessKey, nil
}

func firstRegexMatch(re *regexp.Regexp, input string) string {
	matches := re.FindStringSubmatch(input)
	if len(matches) == 0 {
		return ""
	}
	for idx := 1; idx < len(matches); idx++ {
		if strings.TrimSpace(matches[idx]) != "" {
			return matches[idx]
		}
	}
	return ""
}

func buildSourcesURL(rawURL, dataID, accessKey string) (string, error) {
	logger.Debug("Composing sources URL.")
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""

	trimmedPath := strings.TrimSuffix(parsed.Path, "/")
	if trimmedPath == "" {
		trimmedPath = "/"
	}

	dir := path.Dir(trimmedPath)
	if dir == "." {
		dir = "/"
	}

	parsed.Path = path.Join(dir, "getSources")
	query := url.Values{}
	query.Set("id", dataID)
	query.Set("_k", accessKey)
	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}

func firstHlsSourceURL(items []sourceItem) (string, error) {
	logger.Debug("Finding HLS source entry.")
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.Type), "hls") && strings.TrimSpace(item.File) != "" {
			return strings.TrimSpace(item.File), nil
		}
	}
	return "", errors.New("no hls source found")
}

func defaultSubtitlesSourceURL(items []trackItem) (string, error) {
	logger.Debug("Finding subtitle track entry.")
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.Kind), "captions") && item.Default && strings.TrimSpace(item.File) != "" {
			return strings.TrimSpace(item.File), nil
		}
	}
	return "", errors.New("no subtitle source found")
}

func originAndReferer(inputURL string) (string, string, error) {
	logger.Debug("Deriving origin and referer.")
	parsed, err := url.Parse(inputURL)
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", "", errors.New("input url missing scheme or host")
	}

	origin := parsed.Scheme + "://" + parsed.Host
	referer := origin + "/"
	return origin, referer, nil
}

func selectHighestResolutionPlaylist(masterBody string, masterURL *url.URL) (string, error) {
	logger.Debug("Selecting highest resolution stream.")
	lines := strings.Split(masterBody, "\n")
	bestPixels := -1
	bestURI := ""

	for idx := 0; idx < len(lines); idx++ {
		line := strings.TrimSpace(lines[idx])
		if !strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
			continue
		}

		resolution := parseResolution(line)
		if resolution == 0 {
			continue
		}

		nextIdx := idx + 1
		for nextIdx < len(lines) && strings.TrimSpace(lines[nextIdx]) == "" {
			nextIdx++
		}
		if nextIdx >= len(lines) {
			continue
		}

		uri := strings.TrimSpace(lines[nextIdx])
		if uri == "" || strings.HasPrefix(uri, "#") {
			continue
		}

		if resolution > bestPixels {
			bestPixels = resolution
			bestURI = uri
		}
	}

	if bestURI == "" {
		return "", errors.New("no variant playlist found")
	}

	playlistURL, err := resolvePlaylistURL(masterURL, bestURI)
	if err != nil {
		return "", err
	}

	return playlistURL, nil
}

func parseResolution(line string) int {
	logger.Debug("Parsing resolution line.", zap.String("line", line))
	idx := strings.Index(line, "RESOLUTION=")
	if idx == -1 {
		return 0
	}
	value := line[idx+len("RESOLUTION="):]
	if comma := strings.Index(value, ","); comma != -1 {
		value = value[:comma]
	}
	parts := strings.Split(strings.TrimSpace(value), "x")
	if len(parts) != 2 {
		return 0
	}
	width, err := strconv.Atoi(parts[0])
	if err != nil || width <= 0 {
		return 0
	}
	height, err := strconv.Atoi(parts[1])
	if err != nil || height <= 0 {
		return 0
	}
	return width * height
}

func resolvePlaylistURL(baseURL *url.URL, uri string) (string, error) {
	logger.Debug("Resolving playlist URL.", zap.String("uri", uri))
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	if parsed.IsAbs() {
		return parsed.String(), nil
	}
	return baseURL.ResolveReference(parsed).String(), nil
}

func downloadEpisode(ctx context.Context, videoUrl, outputKey, origin, referer string) (string, error) {
	videoUrl = strings.TrimSpace(videoUrl)
	origin = strings.TrimSpace(origin)
	referer = strings.TrimSpace(referer)

	if videoUrl == "" {
		return "", errors.New("videoUrl is required")
	}
	if origin == "" {
		return "", errors.New("origin is required")
	}
	if referer == "" {
		return "", errors.New("referer is required")
	}

	outputPath := fmt.Sprintf("/tmp/animey_%d.mp4", time.Now().UnixNano())

	headers := fmt.Sprintf("Origin: %s\r\nReferer: %s\r\n", origin, referer)

	args := []string{
		"-extension_picky", "0",
		"-protocol_whitelist", "file,http,https,tcp,tls",
		"-allowed_segment_extensions", "ALL",
		"-headers", headers,
		"-user_agent", userAgentHeader,
		"-icy", "0",
		"-i", videoUrl,
		"-c:v", "copy",
		"-c:a", "copy",
		outputPath,
	}

	logger.Sugar().Debugf("Executing command: ffmpeg %s", strings.Join(args, " "))
	cmd := exec.Command("ffmpeg", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("ffmpeg failed.", zap.Error(err), zap.ByteString("output", out))
		return "", fmt.Errorf("ffmpeg failed: %w: %s", err, string(out))
	}
	logger.Debug("ffmpeg completed.", zap.String("output_path", outputPath))

	logger.Debug("Uploading to S3...", zap.String("file_path", outputPath))
	s3OutputPath, err := s3MultipartUpload(ctx, outputPath, s3OutputBucket, outputKey)
	if err != nil {
		logger.Error("Uploading to S3 failed.", zap.Error(err))
		return "", err
	}

	return s3OutputPath, nil
}

func downloadSubtitles(ctx context.Context, subtitlesUrl, outputKey, origin, referer string) (string, error) {
	subtitlesUrl = strings.TrimSpace(subtitlesUrl)
	origin = strings.TrimSpace(origin)
	referer = strings.TrimSpace(referer)

	if subtitlesUrl == "" {
		return "", errors.New("subtitlesUrl is required")
	}
	if origin == "" {
		return "", errors.New("origin is required")
	}
	if referer == "" {
		return "", errors.New("referer is required")
	}

	outputPath := fmt.Sprintf("/tmp/animey_%d.srt", time.Now().UnixNano())

	headers := fmt.Sprintf("Origin: %s\r\nReferer: %s\r\n", origin, referer)

	args := []string{
		"-protocol_whitelist", "file,http,https,tcp,tls",
		"-headers", headers,
		"-user_agent", userAgentHeader,
		"-icy", "0",
		"-i", subtitlesUrl,
		"-c:s", "srt",
		outputPath,
	}

	logger.Sugar().Debugf("Executing command: ffmpeg %s", strings.Join(args, " "))
	cmd := exec.Command("ffmpeg", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("ffmpeg failed.", zap.Error(err), zap.ByteString("output", out))
		return "", fmt.Errorf("ffmpeg failed: %w: %s", err, string(out))
	}
	logger.Debug("ffmpeg completed.", zap.String("output_path", outputPath))

	logger.Debug("Uploading to S3...", zap.String("file_path", outputPath))
	s3OutputPath, err := s3PutObject(ctx, outputPath, s3OutputBucket, outputKey)
	if err != nil {
		logger.Error("Uploading to S3 failed.", zap.Error(err))
		return "", err
	}

	return s3OutputPath, nil
}

func s3PutObject(ctx context.Context, filePath, bucket, key string) (string, error) {
	if s3Client == nil {
		return "", errors.New("s3 client is not initialized")
	}
	if strings.TrimSpace(filePath) == "" {
		return "", errors.New("file path is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return "", errors.New("bucket is required")
	}
	if strings.TrimSpace(key) == "" {
		return "", errors.New("key is required")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("stat file: %w", err)
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(fi.Size()),
		StorageClass:  s3types.StorageClass(s3OutputStorageClass),
	})
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}

	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

func s3MultipartUpload(ctx context.Context, filePath, bucket, key string) (string, error) {
	if s3Client == nil {
		return "", errors.New("s3 client is not initialized")
	}
	if strings.TrimSpace(filePath) == "" {
		return "", errors.New("file path is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return "", errors.New("bucket is required")
	}
	if strings.TrimSpace(key) == "" {
		return "", errors.New("key is required")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	const partSize = 5 * 1024 * 1024 // 5MB minimum

	createOutput, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: s3types.StorageClass(s3OutputStorageClass),
	})
	if err != nil {
		return "", fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := aws.ToString(createOutput.UploadId)

	completedParts := make([]s3types.CompletedPart, 0)
	partNumber := int32(1)

	buf := make([]byte, partSize)
	for {
		n, readErr := io.ReadFull(f, buf)
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			_ = abortMultipartUpload(ctx, bucket, key, uploadID)
			return "", fmt.Errorf("read file: %w", readErr)
		}

		if n == 0 {
			break
		}

		partBody := bytes.NewReader(buf[:n])
		uploadOut, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          partBody,
			ContentLength: aws.Int64(int64(n)),
		})
		if err != nil {
			_ = abortMultipartUpload(ctx, bucket, key, uploadID)
			return "", fmt.Errorf("upload part %d: %w", partNumber, err)
		}

		completedParts = append(completedParts, s3types.CompletedPart{
			ETag:       uploadOut.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	if len(completedParts) == 0 {
		_ = abortMultipartUpload(ctx, bucket, key, uploadID)
		return "", errors.New("no parts uploaded")
	}

	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		_ = abortMultipartUpload(ctx, bucket, key, uploadID)
		return "", fmt.Errorf("complete multipart upload: %w", err)
	}

	return fmt.Sprintf("s3://%s/%s", bucket, key), nil
}

func abortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if s3Client == nil {
		return errors.New("s3 client is not initialized")
	}
	if strings.TrimSpace(uploadID) == "" {
		return errors.New("upload id is required")
	}
	_, err := s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}
