package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"

	"github.com/gpstream/gpstream/internal/pgtest"
)

func main() {
	cl, err := pgtest.Start(5457, "gptest")
	if err != nil {
		fmt.Println("start err:", err)
		os.Exit(1)
	}
	defer cl.Stop()

	c, err := pgx.Connect(context.Background(), cl.URL())
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Close(context.Background())

	for _, sql := range []string{
		`CREATE TABLE a (id integer, val text)`,
		`INSERT INTO a VALUES (1,'x'),(2,'y'),(3,'z')`,
		`CREATE TABLE b (id integer, val text)`,
		`INSERT INTO b SELECT * FROM a`,
	} {
		_, err := c.Exec(context.Background(), sql)
		if err != nil {
			fmt.Println("exec:", sql, err)
			os.Exit(1)
		}
	}

	getCopy := func(sql string) []byte {
		var buf bytes.Buffer
		_, err := c.PgConn().CopyTo(context.Background(), &buf, sql)
		if err != nil {
			panic(err)
		}
		return buf.Bytes()
	}

	abin, bbin := getCopy("COPY a TO STDOUT (FORMAT binary)"), getCopy("COPY b TO STDOUT (FORMAT binary)")
	atab, tbb := getCopy("COPY a TO STDOUT"), getCopy("COPY b TO STDOUT")

	fmt.Printf("BINARY: a=%s b=%s  match=%v (identical bytes=%v)\n", hexMD5(abin), hexMD5(bbin), hexMD5(abin) == hexMD5(bbin), bytes.Equal(abin, bbin))
	fmt.Printf("TEXT:   a=%s b=%s  match=%v\n", hexMD5(atab), hexMD5(tbb), hexMD5(atab) == hexMD5(tbb))
}

func hexMD5(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
