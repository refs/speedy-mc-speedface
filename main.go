package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/araddon/dateparse"
	"github.com/lib/pq"
)

// TODO
// get biggest                                   [ID] and work with the offset
// insert limits for coupons with ID greater than ^

func main() {
	file, err := os.Open("vuclip_codes.csv")
	if err != nil {
		log.Panic(err)
	}

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

		value := record[0]
		startdate, err := dateparse.ParseAny(record[1])
		expirydate, err := dateparse.ParseAny(record[2])
		campaignid := record[3]

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
