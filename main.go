package main

import (
	"flag"
	"log"
	"time"

	"github.com/CrocSwap/analytics-server-go/job_runner"
	"github.com/CrocSwap/analytics-server-go/loader"
	"github.com/CrocSwap/analytics-server-go/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var listenAddr = flag.String("listenAddr", "0.0.0.0:8080", "listen address")
	var netCfgPath = flag.String("netCfg", "./config/networks.json", "network config file")
	var skipWarmup = flag.Bool("skipWarmup", false, "skip cache warmup")
	var disablePoolStats = flag.Bool("disablePoolStats", false, "disable pool stats worker from filling cache")
	flag.Parse()

	log.Println("Starting analytics server at", *listenAddr)
	netCfg := loader.LoadNetworkConfig(*netCfgPath)
	jobRunner := job_runner.NewJobRunner(netCfg, *disablePoolStats)
	done := make(chan bool)

	if !*skipWarmup {
		go func() {
			jobRunner.WarmUpCache()
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(50 * time.Second):
			log.Println("Cache warmup timed out!")
		}
	}

	apiServer := server.APIWebServer{JobRunner: jobRunner}
	apiServer.Serve("analytics", *listenAddr)
}
