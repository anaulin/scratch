package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", DefaultHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
