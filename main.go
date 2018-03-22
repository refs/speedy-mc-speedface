package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"

	"github.com/araddon/dateparse"
	"github.com/lib/pq"
)

var (
	idOffset = 0
	swaps    = 0
)

func commitBuffer(buffer [][]string, conn *sql.DB, wg *sync.WaitGroup) {
	txn, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("coupons", "startdate", "value", "expirydate", "campaignid", "accountid", "applicationid"))
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(buffer); i++ {
		if len(buffer[i]) == 0 {
			break
		}
		value := buffer[i][0]
		startdate, _ := dateparse.ParseAny(buffer[i][1])
		expirydate, _ := dateparse.ParseAny(buffer[i][2])
		campaignid := buffer[i][3]

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
	wg.Done()
}

func main() {
	var wg sync.WaitGroup
	file, err := os.Open("production.csv")
	if err != nil {
		log.Panic(err)
	}

	conn, err := sql.Open("postgres", "user=alextalon dbname=vuclip host=localhost port=5432 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	conn.SetMaxOpenConns(70)
	conn.SetMaxIdleConns(70)
	defer conn.Close()

	row := conn.QueryRow("SELECT id FROM coupons c ORDER BY c.id DESC LIMIT 1;")
	row.Scan(&idOffset)

	buffer := make([][]string, 0)
	r := csv.NewReader(file)
	for {
		record, err := r.Read()
		if err == io.EOF {
			wg.Add(1)
			go commitBuffer(buffer, conn, &wg)
			break
		}
		if len(buffer) < 10000 {
			buffer = append(buffer, record)
		} else {
			wg.Add(1)
			buffer = append(buffer, record)
			go commitBuffer(buffer, conn, &wg)
			buffer = nil
		}
	}

	wg.Wait()

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

	_, err = conn.Exec(q, idOffset)
	if err != nil {
		log.Panic(err)
	}
}
