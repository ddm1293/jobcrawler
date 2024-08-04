package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/chromedp"
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
	outputDir := "./ibm_jobs/"
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()

	url := "https://www.ibm.com/careers/search"
	var jobListings []string
	err := chromedp.Run(ctx, chromedp.Tasks{
		chromedp.Navigate(url),
		chromedp.WaitVisible(".bx--card-group__cards__col"),
		chromedp.Evaluate(`Array.from(document.querySelectorAll('.bx--card-group__cards__col')).map(e => e.outerHTML)`, &jobListings),
	})
	log.Println("Starting scraping at ", url)
	if err != nil {
		log.Fatalf("Failed to complete chromedp tasks: %v", err)
		return
	}
	log.Println("HTML content retrieved by chromedp")

	for _, jobHTML := range jobListings {
		job, err := extractJobInfo(jobHTML)
		if err != nil {
			log.Printf("Error extracting job info: %v", err)
			continue
		}
		if job.Title != "" && job.Location != "" && job.URL != "" {
			if err := saveJobData(job, outputDir); err != nil {
				log.Printf("Error saving job data: %v", err)
			}
		}
	}

	log.Println("Scraping completed.")
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
	log.Println("see job", job)
	return job, nil
}

func saveJobData(job Job, outputDir string) error {
	jobFile := strings.ReplaceAll(job.Title, " ", "_") + ".json"
	jobFile = strings.ReplaceAll(jobFile, "/", "_")
	jobFilePath := outputDir + jobFile
	file, err := os.Create(jobFilePath)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(job)
	if err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}
	return nil
}
