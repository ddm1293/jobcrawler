package main

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
	"io"
	"log"
	"os"
	"strings"
)

type Job struct {
	Title           string `json:"title"`
	Location        string `json:"location"`
	Description     string `json:"description"`
	ExperienceLevel string `json:"experience_level"`
	URL             string `json:"url"`
}

func main() {
	// open the file
	outputFile := "ibm_jobs.csv"
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close output file: %v", err)
		}
	}(file)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"Title", "Location", "Description", "ExperienceLevel", "URL"}); err != nil {
		log.Fatalf("Failed to write CSV header: %v", err)
	}

	// scrap
	scrapingCtx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	startUrl := "https://www.ibm.com/careers/search?p=1"
	totalItems := 0
	pageNumber := 1

	url := startUrl
	for {
		var jobListings []string
		var nextPageDisabled bool

		err := chromedp.Run(scrapingCtx, chromedp.Tasks{
			chromedp.Navigate(url),
			chromedp.WaitVisible(".bx--card-group__cards__col"),
			chromedp.Evaluate(`Array.from(document.querySelectorAll('.bx--card-group__cards__col')).map(e => e.outerHTML)`, &jobListings),
			chromedp.Evaluate(`document.querySelector('a[data-key="next"][aria-disabled="true"]') !== null`, &nextPageDisabled),
		})

		log.Printf("Starting scraping at page: %d, URL: %s, count: %d", pageNumber, url, totalItems)
		if err != nil {
			log.Fatalf("Failed to complete chromedp tasks: %v", err)
			return
		}

		for _, jobHTML := range jobListings {
			job, err := extractJobInfo(jobHTML)
			if err != nil {
				log.Printf("Error extracting job info: %v", err)
				continue
			}
			if job.Title != "" && job.Location != "" && job.URL != "" {
				if err := writer.Write([]string{job.Title, job.Location, job.Description, job.ExperienceLevel, job.URL}); err != nil {
					log.Fatalf("Failed to write job to CSV: %v", err)
				}
				totalItems++
			}
		}

		if nextPageDisabled {
			break
		}

		pageNumber++
		url = fmt.Sprintf("https://www.ibm.com/careers/search?p=%d", pageNumber)
	}
	log.Println("Scraping completed. Total jobs extracted: ", totalItems)

	// Upload CSV file to Google Cloud Storage
	ctx := context.Background()
	bucketName := "ibm_jobs_bucket"
	if err := uploadFileToGCS(ctx, bucketName, outputFile, outputFile); err != nil {
		log.Fatalf("Failed to upload file to GCS: %v", err)
	}

	// Load CSV file from GCS into BigQuery
	projectID := "jobcrawler-391820"
	datasetID := "ibm_jobs"
	tableID := "ibm_jobs_main_table"
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	if err := loadCSVFromGCS(ctx, client, datasetID, tableID, bucketName, outputFile); err != nil {
		log.Fatalf("Failed to load CSV into BigQuery: %v", err)
	}
}

func extractJobInfo(html string) (Job, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return Job{}, fmt.Errorf("error parsing HTML: %w", err)
	}

	title := doc.Find(".bx--card__heading").Text()
	locationLevel, err := doc.Find(".ibm--card__copy__inner").Html()
	if err != nil {
		return Job{}, fmt.Errorf("error extracting HTML: %w", err)
	}
	url, exists := doc.Find("a.bx--card-group__card").Attr("href")
	if !exists {
		return Job{}, fmt.Errorf("error finding URL")
	}

	locationParts := strings.Split(locationLevel, "<br/>")
	if len(locationParts) < 2 {
		return Job{}, fmt.Errorf("invalid location/level format")
	}
	level := strings.TrimSpace(locationParts[0])
	location := strings.TrimSpace(locationParts[1])

	job := Job{
		Title:           strings.TrimSpace(title),
		Location:        location,
		Description:     "to be implemented",
		ExperienceLevel: level,
		URL:             url,
	}
	return job, nil
}

func uploadFileToGCS(ctx context.Context, bucketName, objectName, filePath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	w := object.NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("failed to copy file to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

func loadCSVFromGCS(ctx context.Context, client *bigquery.Client, datasetID, tableID, bucketName, filePath string) error {
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", bucketName, filePath))
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.FieldDelimiter = ","
	gcsRef.SkipLeadingRows = 1

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to start load job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job did not complete successfully: %w", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("job completed with error: %w", err)
	}

	return nil
}
