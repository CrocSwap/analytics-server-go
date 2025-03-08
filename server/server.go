package server

import (
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/CrocSwap/analytics-server-go/job_runner"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

type APIWebServer struct {
	JobRunner *job_runner.JobRunner
}

func (s *APIWebServer) Serve(prefix string, listenAddress string) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.Use(CORSMiddleware())
	r.Use(gzip.Gzip(gzip.DefaultCompression))
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
	resp, meta, err := s.JobRunner.RunJob(queryMap, rawData)
	if err != nil {
		log.Println("Error running job", err)
	}
	notModified := s.setCacheHeaders(c, meta.MaxAgeSecs, meta.LastModified)
	if notModified {
		return
	}
	wrapPrecompDataErrResp(c, resp, err)
}

// Sets Cache-Control and Last-Modified headers for the response, and optionally checks the
// If-Modified-Since header and sets status to 304 if the content has not changed.
func (s *APIWebServer) setCacheHeaders(c *gin.Context, maxAgeSecs int, lastModified int) (notModified bool) {
	c.Header("Cache-Control", "public, max-age="+strconv.Itoa(maxAgeSecs))
	if lastModified <= 0 {
		return false
	}
	modifiedDate := time.Unix(int64(lastModified), 0).Format("Mon, 02 Jan 2006 15:04:05 MST")
	c.Header("Last-Modified", modifiedDate)
	if c.Request.Header.Get("If-Modified-Since") != "" {
		headerTime, err := time.Parse("Mon, 02 Jan 2006 15:04:05 MST", c.Request.Header.Get("If-Modified-Since"))
		if err != nil {
			log.Println("Error parsing header time", err)
			headerTime = time.Time{}
		}
		if headerTime.Unix() >= int64(lastModified) {
			c.Status(http.StatusNotModified)
			return true
		}
	}
	return false
}
