package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/olivere/elastic"
	"github.com/teris-io/shortid"
)

var (
	elasticClient *elastic.Client
)

const (
	elasticIndexName = "documents"
	elasticTypeName  = "document"
)

type Document struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	Content   string    `json:"content"`
}

type DocumentRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

func main() {
	var err error
	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://localhost:9200/"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}
	app := fiber.New()
	api := app.Group("/")
	ht := api.Group("/health")
	es := api.Group("/api")
	es.Post("/create-documents", createDocuments)
	ht.Get("/health", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	log.Fatal(app.Listen(":3101"))
}

func createDocuments(ct *fiber.Ctx) error {
	// Parse request
	var docs []DocumentRequest
	if err := ct.BodyParser(&docs); err != nil {
		fmt.Println(err)
	}
	// Insert documents in bulk
	bulk := elasticClient.
		Bulk().
		Index(elasticIndexName).
		Type(elasticTypeName)
	for _, d := range docs {
		doc := Document{
			ID:        shortid.MustGenerate(),
			Title:     d.Title,
			CreatedAt: time.Now().UTC(),
			Content:   d.Content,
		}
		bulk.Add(elastic.NewBulkIndexRequest().Id(doc.ID).Doc(doc))
	}
	if _, err := bulk.Do(ct.Context()); err != nil {
		log.Println(err)
		fmt.Println(err)

	}
	return ct.Status(200).JSON(&fiber.Map{
		"success": true,
		"message": "Document create successfully!",
	})

}
