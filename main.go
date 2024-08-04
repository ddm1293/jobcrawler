package main

import (
	"encoding/json"
	"github.com/gocolly/colly/v2"
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
	c := colly.NewCollector(
		colly.AllowURLRevisit(),
		colly.Async(true),
	)

	outputDir := "./ibm_jobs/"
	os.MkdirAll(outputDir, os.ModePerm)

	var elementCount int

	c.OnHTML("div#leadspace-container-0f9ecbdc89.cmp-container", func(e *colly.HTMLElement) {
		log.Println("Element found:", e.Text)
	})

	c.OnHTML(".bx--card-group__cards__col", func(e *colly.HTMLElement) {
		elementCount++
		title := e.ChildText(".bx--card__heading")
		locationLevel := e.ChildText(".ibm--card__copy__inner")
		url := e.ChildAttr("a.bx--card-group__card", "href")

		if title == "" || locationLevel == "" || url == "" {
			log.Println("Missing required fields in the HTML element")
			return
		}

		log.Printf("Title: %s, Location: %s, URL: %s", title, locationLevel, url)

		locationParts := strings.Split(strings.TrimSpace(locationLevel), "\n")
		level := strings.TrimSpace(locationParts[0])
		location := strings.TrimSpace(locationParts[1])

		job := Job{
			Title:           strings.TrimSpace(title),
			Location:        location,
			Description:     "", // Assuming no description available in the snippet
			ExperienceLevel: level,
			URL:             url,
		}

		saveJobData(job, outputDir)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Error:", err)
	})

	url := "https://www.ibm.com/careers/search"
	log.Println("Starting scraping at ", url)
	err := c.Visit(url)
	if err == nil {
		log.Println("Finished scraping at ", url)
	}
	if err != nil {
		log.Fatalf("Failed to start scraping: %v", err)
	}

	c.Wait()

	if elementCount == 0 {
		log.Println("No elements matching '.bx--card-group__cards__col' were found on the page")
	}
}

func saveJobData(job Job, outputDir string) {
	jobFile := strings.ReplaceAll(job.Title, " ", "_") + ".json"
	jobFile = strings.ReplaceAll(jobFile, "/", "_")
	jobFilePath := outputDir + jobFile
	file, err := os.Create(jobFilePath)
	if err != nil {
		log.Println("Could not create file:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(job)
	if err != nil {
		log.Println("Could not write to file:", err)
	}
}
