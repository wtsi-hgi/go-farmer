package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	es "github.com/elastic/go-elasticsearch/v7"
	"gopkg.in/yaml.v3"
)

const index = "user-data-ssg-isg-lsf-analytics-*"

type Config struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := Config{}

	err = yaml.Unmarshal([]byte(data), &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	cfg := es.Config{
		Addresses: []string{
			fmt.Sprintf("%s://%s:%d", c.Elastic.Scheme, c.Elastic.Host, c.Elastic.Port),
		},
		Username: c.Elastic.Username,
		Password: c.Elastic.Password,
	}

	client, err := es.NewClient(cfg)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	res, err := client.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	defer res.Body.Close()
	log.Println(res)

	// es.Search().
	// 	Index("").
	// 	Request(&search.Request{
	// 		Query: &types.Query{MatchAll: &types.MatchAllQuery{}},
	// 	}).
	// Do(context.TODO())

	query := `{ "query": { "match_all": {} } }`
	x, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		log.Fatalf("Error doing search: %s", err)
	}

	fmt.Printf("%+v\n", x)
}
