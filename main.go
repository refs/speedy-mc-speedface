package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/araddon/dateparse"
	"github.com/lib/pq"
)

var (
	file, _    = os.Open("final promocodes.csv")
	strBuilder strings.Builder
	wg         sync.WaitGroup
	queries    string
	counter    int64
	id         = 0
)

func main() {
	conn, err := sql.Open("postgres", "user=alextalon dbname=vuclip host=localhost port=5432 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	txn, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("coupons", "startdate", "value", "expirydate", "campaignid", "accountid", "applicationid"))
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(file)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		startdate, err := dateparse.ParseAny(record[1])
		value := record[0]
		expirydate, err := dateparse.ParseAny(record[3])
		campaignid := record[2]
		if err != nil {
			log.Panic(err)
		}
		_, err = stmt.Exec(startdate, value, expirydate, campaignid, 1, 4)
		if err != nil {
			log.Panic(err)
		}
	}
	err = stmt.Close()
	if err != nil {
		log.Panic(err)
	}
	err = txn.Commit()
	if err != nil {
		txn.Rollback()
		log.Panic(err)
	}
}
