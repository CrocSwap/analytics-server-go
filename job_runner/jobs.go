package job_runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
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
	store.loader.StartPoolStatsWorker()
	return store
}

func (r *JobRunner) RunJob(queryMap map[string]string, jobData []byte) (resp []byte, err error) {
	// If the query parameters don't look like a batch request, run the job directly
	if queryMap["config_path"] != "" && queryMap["config_path"] != "batch_requests" {
		job, err := parseJobFromQueryMap(queryMap)
		if err != nil {
			return nil, err
		}
		results, err := r.RunBatch([]Job{job}, 5*time.Second)
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

	results, err := r.RunBatch(req.Data.Req, DEFAULT_JOB_TIMEOUT)
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

const DEFAULT_JOB_TIMEOUT = 10 * time.Second

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

func (r *JobRunner) RunBatch(jobs []Job, timeout time.Duration) (results []JobResult, err error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Channel to collect job resultChan
	resultChan := make(chan JobResult, len(jobs))

	// Start a goroutine for each job
	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Println("job timed out", r)
				}

			}()
			result, err := r.Execute(job)
			if err != nil {
				log.Printf("Error executing job %s: %v", string(job.Args), err)
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
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, err
		}
		resp, err = r.loader.GetEns(args.Address)
	case "price":
		var args loader.PriceArgs
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, err
		}
		resp, _, err = r.loader.GetPrice(args)
	case "all_pool_stats":
		var args loader.PoolLoc
		err = json.Unmarshal(j.Args, &args)
		if err != nil {
			return nil, err
		}
		resp = r.loader.GetAllPoolStats(args)
	default:
		return nil, fmt.Errorf("unknown job type: %s", j.ConfigPath)
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
	switch job.ConfigPath {
	case "ens_address":
		job.Args = json.RawMessage(fmt.Sprintf(`{"address": "%s"}`, queryMap["address"]))
	case "price":
		job.Args = json.RawMessage(fmt.Sprintf(`{"asset_platform": "%s", "token_address": "%s"}`, queryMap["asset_platform"], queryMap["token_address"]))
	case "pool_stats":
	case "all_pool_stats":
		poolIdx, _ := strconv.Atoi(queryMap["poolIdx"])
		job.Args = json.RawMessage(fmt.Sprintf(`{"chainId": "%s", "base": "%s", "quote": "%s", "poolIdx": %s}`, queryMap["chainId"], queryMap["base"], queryMap["quote"], strconv.Itoa(poolIdx)))
		log.Println("Job args:", string(job.Args))
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

			_, err := r.RunBatch(batch, DEFAULT_JOB_TIMEOUT)
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
