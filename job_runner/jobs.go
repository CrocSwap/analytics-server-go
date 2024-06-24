package job_runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/loader"
)

type Request struct {
	Service    string `json:"service"`
	ConfigPath string `json:"config_path"`
	Data       Jobs   `json:"data"`
}

type Jobs struct {
	Req []Job `json:"req"`
}

type Job struct {
	ConfigPath string          `json:"config_path"`
	ReqID      string          `json:"req_id"`
	Args       json.RawMessage `json:"args"`
}

type JobRunner struct {
	jobChannel chan *Request
	loader     *loader.Loader
}

func NewJobRunner(netCfg loader.NetworkConfig) *JobRunner {
	jobChannel := make(chan *Request)
	store := &JobRunner{
		jobChannel: jobChannel,
		loader:     loader.NewLoader(netCfg),
	}
	return store
}

func (r *JobRunner) RunJob(queryMap map[string]string, jobData []byte) (resp []byte, err error) {
	// If the query parameters don't look like a batch request, run the job directly
	if queryMap["config_path"] != "" && queryMap["config_path"] != "batch_requests" {
		job, err := parseJobFromQueryMap(queryMap)
		if err != nil {
			return nil, err
		}
		results, err := r.RunBatch([]Job{job})
		if err != nil {
			return nil, err
		}
		return results[0].Result, nil
	}

	var req Request
	err = json.Unmarshal(jobData, &req)
	if err != nil {
		return nil, err
	}

	results, err := r.RunBatch(req.Data.Req)
	if err != nil {
		return nil, err
	}
	var response Response
	for _, result := range results {
		var jobResp JobResultResponse
		jobResp.ReqID = result.ID
		if result.Err != nil {
			errResp, err := json.Marshal("internal error") // to prevent leaking errors to users
			if err != nil {
				log.Println("Error marshalling error response", err)
				return nil, err
			}
			jobResp.Error = json.RawMessage(errResp)
		} else {
			jobResp.Results = json.RawMessage(result.Result)
		}
		response.Value.Data = append(response.Value.Data, jobResp)
	}

	resp, err = json.Marshal(response)
	if err != nil {
		return nil, err
	}

	return
}

const JOB_TIMEOUT = 10 * time.Second

type JobResult struct {
	ID     string
	Result []byte
	Err    error
}

type Response struct {
	Value struct {
		Data []JobResultResponse `json:"data"`
	} `json:"value"`
}

type JobResultResponse struct {
	ReqID   string          `json:"req_id"`
	Results json.RawMessage `json:"results,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func (r *JobRunner) RunBatch(jobs []Job) (results []JobResult, err error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), JOB_TIMEOUT)
	defer cancel()

	// Channel to collect job resultChan
	resultChan := make(chan JobResult, len(jobs))

	// Start a goroutine for each job
	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer func() {
				wg.Done()
				recover() // channel will panic after timeout
			}()
			result, err := r.Execute(job)
			if err != nil {
				log.Printf("Error executing job %s: %v", job.ReqID, err)
			}
			resultChan <- JobResult{ID: job.ReqID, Result: result, Err: err}

		}(job)
	}

	resultMap := map[string]JobResult{}
outer:
	for {
		select {
		case <-ctx.Done():
			close(resultChan)
			break outer
		case result := <-resultChan:
			results = append(results, result)
			resultMap[result.ID] = result
			if len(resultMap) == len(jobs) {
				break outer
			}
		}
	}

	for _, job := range jobs {
		if _, ok := resultMap[job.ReqID]; !ok {
			results = append(results, JobResult{ID: job.ReqID, Err: errors.New("timed out")})
		}
	}

	return results, nil
}

func (r *JobRunner) Execute(j Job) (resp []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("panic in job", r)
			err = errors.New("internal error")
		}
	}()
	switch j.ConfigPath {
	case "ens_address":
		var args loader.EnsArgs
		err := json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, err
		}
		resp, err = r.loader.GetEns(args.Address)
	case "price":
		var args loader.PriceArgs
		err := json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, err
		}
		resp, err = r.loader.GetPrice(args)
	default:
		return nil, fmt.Errorf("unknown job type: %s", j.ConfigPath)
	}

	if resp == nil {
		resp = []byte("{\"error\": \"internal error\"}")
	}
	return
}

func parseJobFromQueryMap(queryMap map[string]string) (job Job, err error) {
	job = Job{
		ConfigPath: queryMap["config_path"],
	}
	switch job.ConfigPath {
	case "ens_address":
		job.Args = json.RawMessage(fmt.Sprintf(`{"address": "%s"}`, queryMap["address"]))
	case "price":
		job.Args = json.RawMessage(fmt.Sprintf(`{"asset_platform": "%s", "token_address": "%s"}`, queryMap["asset_platform"], queryMap["token_address"]))
	default:
		err = fmt.Errorf("unknown job type: %s", job.ConfigPath)
	}
	return
}
