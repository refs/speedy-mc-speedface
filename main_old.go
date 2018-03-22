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

var (
	idOffset = 0
)

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

	row := conn.QueryRow("SELECT id FROM coupons c ORDER BY c.id DESC LIMIT 1;")
	row.Scan(&idOffset)

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

	q := `INSERT INTO limit_counters (action, campaignid, couponid, counter, limitval)
	(
	  SELECT
		'redeemCoupon' as action,
		c.campaignid as campaignid,
		c.id as couponid,
		0.0 as counter,
		1.0 as limitval
	  FROM
		coupons as c
	  WHERE
		c.id > $1
	)
	`

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
	}

	err = txn.Commit()
	if err != nil {
		txn.Rollback()
		log.Panic(err)
	}
	_, err = conn.Exec(q, idOffset)
	if err != nil {
		log.Panic(err)
	}
}
