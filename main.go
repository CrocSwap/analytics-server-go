package main

import (
	"flag"
	"log"

	"github.com/CrocSwap/analytics-server-go/job_runner"
	"github.com/CrocSwap/analytics-server-go/loader"
	"github.com/CrocSwap/analytics-server-go/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var listenAddr = flag.String("listenAddr", "0.0.0.0:8080", "listen address")
	var netCfgPath = flag.String("netCfg", "./config/ethereum.json", "network config file")
	flag.Parse()

	log.Println("Starting analytics server at", *listenAddr)
	netCfg := loader.LoadNetworkConfig(*netCfgPath)
	jobRunner := job_runner.NewJobRunner(netCfg)
	apiServer := server.APIWebServer{JobRunner: jobRunner}
	apiServer.Serve("analytics", *listenAddr)
}
