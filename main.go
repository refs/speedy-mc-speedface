package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

var (
	file, _    = os.Open("example.csv")
	strBuilder strings.Builder
	wg         sync.WaitGroup
	queries    string
	counter    int64
)

func process(row []string, wg *sync.WaitGroup) {
	strBuilder.WriteString(row[0])
	wg.Done()
}

func main() {
	connStr := "user=talon dbname=talon host=localhost port=5433 password=talon.one.9000 sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM coupons;")
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err != nil {
			log.Fatal(err)
		}
		log.Println("result")
	}

	r := csv.NewReader(file)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		wg.Add(1)
		go process(record, &wg)
	}
	wg.Wait()

}
