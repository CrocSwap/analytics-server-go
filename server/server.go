package server

import (
	"io"
	"log"
	"net/http"

	"github.com/CrocSwap/analytics-server-go/job_runner"
	"github.com/gin-gonic/gin"
)

type APIWebServer struct {
	JobRunner *job_runner.JobRunner
}

func (s *APIWebServer) Serve(prefix string, listenAddress string) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.Use(CORSMiddleware())
	r.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET(prefix+"/", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET(prefix+"/run", s.runJob)
	r.POST(prefix+"/run", s.runJob)

	log.Println("API Serving at", prefix, listenAddress)
	r.Run(listenAddress)
}

func (s *APIWebServer) runJob(c *gin.Context) {
	query := c.Request.URL.Query()
	queryMap := map[string]string{}
	for k, v := range query {
		queryMap[k] = v[0]
	}
	br := c.Request.Body
	defer br.Close()
	rawData, err := io.ReadAll(br)
	if err != nil {
		log.Printf("Request body read error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}
	resp, err := s.JobRunner.RunJob(queryMap, rawData)
	if err != nil {
		log.Println("Error running job", err)
	}
	wrapPrecompDataErrResp(c, resp, err)
}
