package loader

import (
	"hash/fnv"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
)

const ENS_CACHE_TTL = 72 * time.Hour
const PRICE_CACHE_TTL = 2 * time.Minute
const INFINITE_CACHE_TTL = time.Hour * 24 * 365

type Loader struct {
	NetCfg          NetworkConfig
	Cache           map[[16]byte]CacheEntry
	CacheLock       sync.RWMutex
	poolStatsWorker PoolStatsWorker
	httpClient      *http.Client
	ethClients      map[int]bind.ContractBackend
}

type CacheEntry struct {
	ExpiresAt time.Time
	Data      []byte
}

func NewLoader(netCfg NetworkConfig) *Loader {
	if os.Getenv("COINGECKO_API_KEY") == "" {
		panic("COINGECKO_API_KEY env var is required")
	}
	clientMap := make(map[int]bind.ContractBackend)
	for _, chainCfg := range netCfg {
		if chainCfg.RPCEndpoint != "" {
			ethClient := NewBatchedEthClient(chainCfg)
			clientMap[chainCfg.ChainID] = ethClient
		}
	}

	return &Loader{
		NetCfg: netCfg,
		Cache:  map[[16]byte]CacheEntry{},
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		ethClients: clientMap,
	}
}

func (l *Loader) StartPoolStatsWorker() {
	l.poolStatsWorker = PoolStatsWorker{
		loader:    l,
		prices:    map[PriceArgs]float64{},
		poolStats: []PoolStats{},
	}
	go l.poolStatsWorker.RunPoolStatsWorker()
}

func (l *Loader) AddToCache(key string, data []byte, ttl time.Duration) {
	// log.Println("AddToCache", key, string(data))
	hash := fnv.New128a()
	hash.Write([]byte(key))

	l.CacheLock.Lock()
	defer l.CacheLock.Unlock()
	l.Cache[[16]byte(hash.Sum(nil))] = CacheEntry{
		ExpiresAt: time.Now().Add(ttl),
		Data:      data,
	}
}

func (l *Loader) GetFromCache(key string) (data []byte, ok bool) {
	hash := fnv.New128a()
	hash.Write([]byte(key))

	l.CacheLock.RLock()
	defer l.CacheLock.RUnlock()
	entry, ok := l.Cache[[16]byte(hash.Sum(nil))]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry.Data, true
}

func (l *Loader) GetFloat64FromCache(key string) (data float64, ok bool) {
	bytes, ok := l.GetFromCache(key)
	if !ok {
		return
	}

	data, _ = strconv.ParseFloat(string(bytes), 64)
	return
}

func (l *Loader) AddFloat64ToCache(key string, data float64, ttl time.Duration) {
	l.AddToCache(key, []byte(strconv.FormatFloat(data, 'f', -1, 64)), ttl)
}
