package loader

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/types"
)

type PoolStatsWorker struct {
	loader *Loader
	// Pools are interleaved by chain and sorted by the number of events.
	poolStats       []PoolStats
	lastStatsUpdate int64
	gcgoProvenance  map[types.ChainId]string
	// Separate cache of prices that's maintained by `priceWorker`. It's needed
	// because the response must be instant.
	prices     map[PriceArgs]float64
	pricesLock sync.RWMutex
}

const STATS_REFRESH_PERIOD = 10 * time.Second

var assetPlatformMap = map[types.ChainId]string{
	"0x1":     "ethereum",
	"0x783":   "swell",
	"0x13e31": "blast",
	"0x18231": "plume",
	"0x82750": "scroll",
	"0x279f":  "monad",
}

var chainOrder = []types.ChainId{"0x1", "0x82750", "0x13e31", "0x783", "0x18231", "0x279f"}

func (s *PoolStatsWorker) RunPoolStatsWorker() {
	go s.priceWorker()
	for {
		log.Println("Refreshing pool stats")
		longestChain := 0
		poolStatsPerChain := make(map[types.ChainId][]PoolStats)
		gcgoProvenanceMap := make(map[types.ChainId]string)
		lastStatsUpdate := time.Now().UnixMilli()
		for _, chainCfg := range s.loader.NetCfg {
			for retry := 0; retry < 5; retry++ {
				stats, gcgoProvenance, err := FetchAllPoolStats(chainCfg.Graphcache, types.IntToChainId(chainCfg.ChainID))
				if err != nil {
					log.Println("Error fetching pool stats for chain", chainCfg.ChainID, err)
					time.Sleep(5 * time.Second * time.Duration(retry))
					continue
				}
				poolStatsPerChain[types.IntToChainId(chainCfg.ChainID)] = stats
				if len(stats) > longestChain {
					longestChain = len(stats)
				}
				gcgoProvenanceMap[types.IntToChainId(chainCfg.ChainID)] = gcgoProvenance
				break
			}
		}
		newPoolStats := []PoolStats{}
		// To improve price fetching behavior, stats are interleaved by chain.
		for i := 0; i < longestChain; i++ {
			for _, chain := range chainOrder {
				stats := poolStatsPerChain[chain]
				if i < len(stats) {
					newPoolStats = append(newPoolStats, stats[i])
				}
			}
		}
		s.poolStats = newPoolStats
		s.gcgoProvenance = gcgoProvenanceMap
		s.lastStatsUpdate = lastStatsUpdate
		time.Sleep(STATS_REFRESH_PERIOD)
	}
}

// Iterates over all tokens in `poolStats` and refreshes their prices.
func (s *PoolStatsWorker) priceWorker() {
	// Wait until the first pool refresh is complete.
	for {
		time.Sleep(time.Second)
		if len(s.poolStats) > 0 {
			break
		}
	}

	// Wait until after the startup cache completes.
	time.Sleep(30 * time.Second)

	for {
		uniqueTokensMap := make(map[PriceArgs]struct{})
		uniqueTokens := make([]PriceArgs, 0, len(s.poolStats))
		for _, stats := range s.poolStats {
			baseArgs := PriceArgs{
				TokenAddress:  string(stats.Base),
				AssetPlatform: assetPlatformMap[stats.ChainId],
			}
			quoteArgs := PriceArgs{
				TokenAddress:  string(stats.Quote),
				AssetPlatform: assetPlatformMap[stats.ChainId],
			}
			if _, ok := uniqueTokensMap[baseArgs]; !ok {
				uniqueTokensMap[baseArgs] = struct{}{}
				uniqueTokens = append(uniqueTokens, baseArgs)
			}
			if _, ok := uniqueTokensMap[quoteArgs]; !ok {
				uniqueTokensMap[quoteArgs] = struct{}{}
				uniqueTokens = append(uniqueTokens, quoteArgs)
			}
		}

		log.Printf("Refreshing prices for %d tokens", len(uniqueTokens))
		for _, args := range uniqueTokens {
			priceJson, cached, err := s.loader.GetPrice(args)
			if err != nil {
				log.Println("Error refreshing price", args, err)
				continue
			}
			var price PriceResp
			json.Unmarshal(priceJson, &price)
			s.pricesLock.Lock()
			s.prices[args] = price.Value.UsdPrice
			s.pricesLock.Unlock()

			if cached {
				time.Sleep(100 * time.Millisecond)
			} else {
				time.Sleep(1 * time.Second)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func (l *Loader) GetAllPoolStats(loc PoolLoc) (allStatsJson []byte) {
	allStats := make([]PoolStats, 0, len(l.poolStatsWorker.poolStats))
	l.poolStatsWorker.pricesLock.RLock()
	defer l.poolStatsWorker.pricesLock.RUnlock()
	for _, stats := range l.poolStatsWorker.poolStats {
		if (loc.ChainId == "" || loc.ChainId == stats.ChainId) && (loc.Base == "" || loc.Base == stats.Base) && (loc.Quote == "" || loc.Quote == stats.Quote) && (loc.PoolIdx == 0 || loc.PoolIdx == stats.PoolIdx) {
			baseArgs := PriceArgs{
				TokenAddress:  string(stats.Base),
				AssetPlatform: assetPlatformMap[stats.ChainId],
			}
			quoteArgs := PriceArgs{
				TokenAddress:  string(stats.Quote),
				AssetPlatform: assetPlatformMap[stats.ChainId],
			}
			var basePrice *float64
			if price, ok := l.poolStatsWorker.prices[baseArgs]; ok {
				basePrice = &price
			}
			var quotePrice *float64
			if price, ok := l.poolStatsWorker.prices[quoteArgs]; ok {
				quotePrice = &price
			}
			stats.BaseUsdPrice = basePrice
			stats.QuoteUsdPrice = quotePrice
			allStats = append(allStats, stats)
		}
	}

	allStatsJson, err := wrapGcgoLikeDataResp(allStats, l.poolStatsWorker.gcgoProvenance, l.poolStatsWorker.lastStatsUpdate)
	if err != nil {
		log.Panicln("Error marshalling all pool stats", err)
	}
	return
}

type responseProvenance struct {
	Hostname        string                   `json:"hostname"`
	GcgoProvenance  map[types.ChainId]string `json:"gcgoProvenance"`
	LastStatsUpdate int64                    `json:"lastStatsUpdate"`
	ServeTime       int64                    `json:"serveTime"`
}

type fullGcgoLikeResponse struct {
	Data     any                `json:"data"`
	Metadata responseProvenance `json:"provenance"`
}

func wrapGcgoLikeDataResp(result any, gcgoProvenance map[types.ChainId]string, lastStatsUpdate int64) (wrappedRespJson []byte, err error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "getHostnameError"
	}

	prov := responseProvenance{
		Hostname:        hostname,
		GcgoProvenance:  gcgoProvenance,
		LastStatsUpdate: lastStatsUpdate,
		ServeTime:       time.Now().UnixMilli(),
	}

	wrappedResp := fullGcgoLikeResponse{Data: result, Metadata: prov}
	wrappedRespJson, err = json.Marshal(wrappedResp)
	return
}
