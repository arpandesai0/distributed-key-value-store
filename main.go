package main

import (
	"distributed-key-value-store/config"
	"distributed-key-value-store/db"
	"distributed-key-value-store/web"
	"flag"
	"log"
	"net/http"
)

var (
	dbLocation = flag.String("db-location", "", "The path to the bold db database")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "Http host and port")
	configFile = flag.String("config-file", "sharding.toml", "Config file for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Boolean showing whether or not this server is replica server or not")
)

func parsePlags() {
	flag.Parse()

	if *dbLocation == "" {
		log.Fatal("Must provide db-location")
	}

	if *shard == "" {
		log.Fatal("Must provide shard")
	}
}

func main() {
	parsePlags()
	c, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error while parsing config %q: %v", *configFile, err)
	}
	shards, err := config.ParseShards(c.Shards, *shard)
	if err != nil {
		log.Fatalf("Error while parsing shards from config: %v", err)
	}
	db, close, err := db.NewDatabase(*dbLocation, *replica)
	if err != nil {
		log.Fatalf("NewDatabase(%q) :%v", *dbLocation, err)
	}
	defer close()

	server := web.NewServer(db, shards)

	http.HandleFunc("/get", server.GetHandler)
	http.HandleFunc("/set", server.SetHandler)
	http.HandleFunc("/delete-extra", server.DeleteExtraKeys)

	server.ListenAndServe(*httpAddr)
}
