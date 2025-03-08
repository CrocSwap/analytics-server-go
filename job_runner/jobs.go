package job_runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/loader"
	"github.com/CrocSwap/analytics-server-go/types"
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

type JobResult struct {
	ID     string
	Result []byte
	Err    error
	JobResultMeta
}

type JobResultMeta struct {
	LastModified int
	MaxAgeSecs   int
}

type InProgressJob struct {
	Job
	ResultChans []chan JobResult
}

const DEFAULT_JOB_TIMEOUT = 5 * time.Second
const DEFAULT_BATCH_TIMEOUT = 10 * time.Second

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

func (j Job) Hash() [16]byte {
	fnvHash := fnv.New128()
	fnvHash.Write([]byte(j.ConfigPath))
	fnvHash.Write([]byte(j.Args))
	hash := fnvHash.Sum(nil)
	return [16]byte(hash)
}

type JobRunner struct {
	jobChannel         chan *Request
	loader             *loader.Loader
	inProgressJobs     map[[16]byte]*InProgressJob
	inProgressJobsLock sync.Mutex
}

func NewJobRunner(netCfg loader.NetworkConfig, disablePoolStats bool) *JobRunner {
	jobChannel := make(chan *Request)
	store := &JobRunner{
		jobChannel:     jobChannel,
		loader:         loader.NewLoader(netCfg),
		inProgressJobs: map[[16]byte]*InProgressJob{},
	}
	if !disablePoolStats {
		store.loader.StartPoolStatsWorker()
	}
	return store
}

func (r *JobRunner) ScheduleJob(job Job) (resultChan chan JobResult) {
	inProgressJob := &InProgressJob{
		Job:         job,
		ResultChans: []chan JobResult{},
	}
	resultChan = make(chan JobResult, 1)
	hash := job.Hash()
	r.inProgressJobsLock.Lock()
	// If the job is already scheduled, add the new result channel to the existing job,
	// otherwise create a new job and execute it in a goroutine.
	if existingJob, ok := r.inProgressJobs[hash]; ok {
		inProgressJob = existingJob
		inProgressJob.ResultChans = append(inProgressJob.ResultChans, resultChan)
	} else {
		inProgressJob.ResultChans = append(inProgressJob.ResultChans, resultChan)
		r.inProgressJobs[hash] = inProgressJob
		go func() {
			defer func() {
				if p := recover(); p != nil {
					log.Println("panic JobRunner.ScheduleJob:", string(debug.Stack()))
					r.inProgressJobsLock.Lock()
					for _, resultChan := range inProgressJob.ResultChans {
						resultChan <- JobResult{ID: job.ReqID, Err: errors.New("internal error")}
					}
					delete(r.inProgressJobs, hash)
					r.inProgressJobsLock.Unlock()
				}
			}()
			result, resultMeta, err := r.RunScheduled(*inProgressJob, DEFAULT_JOB_TIMEOUT)
			if err != nil {
				log.Printf("Error executing job %v: %s", string(job.Args), err)
			}
			r.inProgressJobsLock.Lock()
			for _, resultChan := range inProgressJob.ResultChans {
				resultChan <- JobResult{ID: job.ReqID, Result: result, Err: err, JobResultMeta: resultMeta}
			}
			delete(r.inProgressJobs, hash)
			r.inProgressJobsLock.Unlock()
		}()
	}
	r.inProgressJobsLock.Unlock()
	return
}

func (r *JobRunner) RunJob(queryMap map[string]string, jobData []byte) (resp []byte, meta JobResultMeta, err error) {
	// If the query parameters don't look like a batch request, run the job directly
	if queryMap["config_path"] != "" && queryMap["config_path"] != "batch_requests" {
		job, err := parseJobFromQueryMap(queryMap)
		if err != nil {
			return nil, meta, err
		}
		resultChan := r.ScheduleJob(job)
		select {
		case result := <-resultChan:
			meta = result.JobResultMeta
			if result.Err != nil {
				return nil, meta, result.Err
			}
			return result.Result, meta, nil
		case <-time.After(DEFAULT_JOB_TIMEOUT):
			return nil, meta, errors.New("timed out")
		}
	}

	var req Request
	err = json.Unmarshal(jobData, &req)
	if err != nil {
		return nil, meta, err
	}

	results, err := r.RunBatch(req.Data.Req, DEFAULT_BATCH_TIMEOUT)
	if err != nil {
		return nil, meta, err
	}
	var response Response
	for _, result := range results {
		var jobResp JobResultResponse
		jobResp.ReqID = result.ID
		if result.Err != nil {
			// To prevent leaking errors to users:
			jobResp.Error = json.RawMessage("\"internal error\"")
		} else {
			jobResp.Results = json.RawMessage(result.Result)
		}
		response.Value.Data = append(response.Value.Data, jobResp)
	}

	resp, err = json.Marshal(response)
	if err != nil {
		return nil, meta, err
	}

	return
}

func (r *JobRunner) RunBatch(jobs []Job, timeout time.Duration) (results []JobResult, err error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Channel to collect job results
	resultChan := make(chan JobResult, len(jobs))

	// Start a goroutine for each job
	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Println("job timed out:", r)
				}

			}()
			jobResultChan := r.ScheduleJob(job)
			select {
			case result := <-jobResultChan:
				resultChan <- JobResult{ID: job.ReqID, Result: result.Result, Err: result.Err}
			case <-ctx.Done():
				resultChan <- JobResult{ID: job.ReqID, Err: errors.New("timed out")}
			}

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
			// log.Println(len(resultMap), len(jobs))
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

func (r *JobRunner) RunScheduled(job InProgressJob, timeout time.Duration) (resp []byte, meta JobResultMeta, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resultChan := make(chan JobResult)

	go func(job Job) {
		defer func() {
			if r := recover(); r != nil {
				log.Println("job timed out:", r)
			}
		}()
		result, meta, err := r.Execute(job)
		if err != nil {
			log.Printf("Error executing job %s: %v", string(job.Args), err)
		}
		resultChan <- JobResult{ID: job.ReqID, Result: result, Err: err, JobResultMeta: meta}

	}(job.Job)

	select {
	case result := <-resultChan:
		return result.Result, result.JobResultMeta, result.Err
	case <-ctx.Done():
		close(resultChan)
		return nil, meta, errors.New("timed out")
	}
}

func (r *JobRunner) Execute(j Job) (resp []byte, resultMeta JobResultMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("panic in JobRunner.Execute:", string(debug.Stack()))
			err = errors.New("internal error")
		}
	}()
	//sleep random time from 0 to 15 seconds
	switch j.ConfigPath {
	case "ens_address":
		var args loader.EnsArgs
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, resultMeta, err
		}
		resultMeta = JobResultMeta{MaxAgeSecs: 48 * 60 * 60}
		resp, err = r.loader.GetEns(args.Address)
	case "price":
		var args loader.PriceArgs
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, resultMeta, err
		}
		resultMeta = JobResultMeta{MaxAgeSecs: 60}
		resp, _, err = r.loader.GetPrice(args)
	case "all_pool_stats":
		var args loader.PoolLoc
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, resultMeta, err
		}
		resultMeta = JobResultMeta{MaxAgeSecs: 30}
		resp = r.loader.GetAllPoolStats(args)
	default:
		return nil, resultMeta, fmt.Errorf("unknown job type: %s", j.ConfigPath)
	}

	if err != nil {
		log.Printf("Error executing job %v: %s", string(j.Args), err)
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
	// ToLower needed here to hash the job consistently
	switch job.ConfigPath {
	case "ens_address":
		address := strings.ToLower(queryMap["address"])
		job.Args = json.RawMessage(fmt.Sprintf(`{"address": "%s"}`, address))
	case "price":
		assetPlatform := strings.ToLower(queryMap["asset_platform"])
		tokenAddress := strings.ToLower(queryMap["token_address"])
		job.Args = json.RawMessage(fmt.Sprintf(`{"asset_platform": "%s", "token_address": "%s"}`, assetPlatform, tokenAddress))
	case "pool_stats":
	case "all_pool_stats":
		poolIdx, _ := strconv.Atoi(queryMap["poolIdx"])
		base := strings.ToLower(queryMap["base"])
		quote := strings.ToLower(queryMap["quote"])
		chainId := strings.ToLower(queryMap["chainId"])
		job.Args = json.RawMessage(fmt.Sprintf(`{"chainId": "%s", "base": "%s", "quote": "%s", "poolIdx": %s}`, chainId, base, quote, strconv.Itoa(poolIdx)))
	default:
		err = fmt.Errorf("unknown job type: %s", job.ConfigPath)
	}
	return
}

// Caches prices for all tokens in non-empty pools, and fetches ENS domains for
// users in latest transactions for top 7 pools (sorted by events)
func (r *JobRunner) WarmUpCache() {
	log.Println("Warming up cache...")
	type chainData struct {
		name          string
		indexer       string
		tokens        map[types.EthAddress]int // value is the number of events for the token
		userAddresses map[types.EthAddress]struct{}
	}
	chains := map[types.ChainId]chainData{}
	log.Println("netCfg:", r.loader.NetCfg)
	for _, cfg := range r.loader.NetCfg {
		chains[types.IntToChainId(cfg.ChainID)] = chainData{
			name:          cfg.NetworkName,
			indexer:       cfg.Graphcache,
			tokens:        map[types.EthAddress]int{},
			userAddresses: map[types.EthAddress]struct{}{},
		}
	}

	log.Println("Chains:", chains)

	wg := sync.WaitGroup{}
	for chainId, chain := range chains {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Warming up cache for %s...", chain.name)
			pools, _, err := loader.FetchAllPoolStats(chain.indexer, chainId)
			if err != nil {
				log.Printf("Error fetching pool stats for chain %s: %v", chainId, err)
				return
			}

			topPools := []loader.PoolStats{}
			for i, pool := range pools {
				if pool.Events > 0 {
					chain.tokens[pool.Base] += pool.Events
					chain.tokens[pool.Quote] += pool.Events
					if i < 7 {
						topPools = append(topPools, pool)
					}
				}
			}

			for _, pool := range topPools {
				txs, err := loader.GetPoolTxs(chain.indexer, pool.PoolLoc)
				if err != nil {
					log.Printf("Error fetching pool txs for pool %v: %v", pool.PoolLoc, err)
				}

				for _, tx := range txs {
					chain.userAddresses[tx.User] = struct{}{}
				}
			}

		}()
	}

	wg.Wait()

	type chainToken struct {
		token types.EthAddress
		chain string
		count int
	}

	allTokens := []chainToken{}
	allUserAddressesMap := map[types.EthAddress]struct{}{}
	for _, chain := range chains {
		for token, events := range chain.tokens {
			allTokens = append(allTokens, chainToken{token, chain.name, events})
		}
		for user := range chain.userAddresses {
			allUserAddressesMap[user] = struct{}{}
		}
	}

	allUserAddresses := []types.EthAddress{}
	for user := range allUserAddressesMap {
		allUserAddresses = append(allUserAddresses, user)
	}

	slices.SortFunc(allTokens, func(i, j chainToken) int {
		return j.count - i.count
	})

	// Goroutine to cache token prices
	wg.Add(1)
	go func() {
		BATCH_SIZE := 20
		for i := 0; i < len(allTokens); i += BATCH_SIZE {
			end := i + BATCH_SIZE
			if end > len(allTokens) {
				end = len(allTokens)
			}

			batch := []Job{}

			for _, token := range allTokens[i:end] {
				job := Job{
					ConfigPath: "price",
					ReqID:      fmt.Sprintf("%s-%s", token.chain, token.token),
					Args:       json.RawMessage(fmt.Sprintf(`{"asset_platform": "%s", "token_address": "%s"}`, token.chain, token.token)),
				}
				batch = append(batch, job)
			}

			_, err := r.RunBatch(batch, DEFAULT_BATCH_TIMEOUT)
			if err != nil {
				log.Printf("Error fetching prices for tokens: %v", err)
			}
			log.Printf("Fetched %d/%d prices", end, len(allTokens))
			time.Sleep(3 * time.Second)
		}
		log.Println("Price cache warmed up")
		wg.Done()
	}()

	// Goroutine to cache ENS domains
	wg.Add(1)
	go func() {
		BATCH_SIZE := 1000
		for i := 0; i < len(allUserAddresses); i += BATCH_SIZE {
			end := i + BATCH_SIZE
			if end > len(allUserAddresses) {
				end = len(allUserAddresses)
			}

			batch := []Job{}
			for _, user := range allUserAddresses[i:end] {
				job := Job{
					ConfigPath: "ens_address",
					ReqID:      string(user),
					Args:       json.RawMessage(fmt.Sprintf(`{"address": "%s"}`, user)),
				}
				batch = append(batch, job)
			}

			_, err := r.RunBatch(batch, 40*time.Second)
			if err != nil {
				log.Printf("Error fetching ENS addresses for users: %v", err)
			}
			log.Printf("Fetched %d/%d ENS addresses", end, len(allUserAddresses))
		}
		log.Println("ENS cache warmed up")
		wg.Done()
	}()

	wg.Wait()

	log.Println("Cache warmed up")
}
