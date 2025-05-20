package loader

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/CrocSwap/analytics-server-go/types"
)

type PoolLoc struct {
	ChainId types.ChainId    `json:"chainId"`
	Base    types.EthAddress `json:"base"`
	Quote   types.EthAddress `json:"quote"`
	PoolIdx int              `json:"poolIdx"`
}

type PoolStats struct {
	PoolLoc
	BaseUsdPrice      *float64 `json:"baseUsdPrice"`
	QuoteUsdPrice     *float64 `json:"quoteUsdPrice"`
	LastPriceSwap     float64  `json:"lastPriceSwap"`
	PriceSwap24HAgo   float64  `json:"priceSwap24hAgo"`
	BaseTvl           float64  `json:"baseTvl"`
	QuoteTvl          float64  `json:"quoteTvl"`
	BaseVolume        float64  `json:"baseVolume"`
	BaseVolume24HAgo  float64  `json:"baseVolume24hAgo"`
	QuoteVolume       float64  `json:"quoteVolume"`
	QuoteVolume24HAgo float64  `json:"quoteVolume24hAgo"`
	BaseFees          float64  `json:"baseFees"`
	BaseFees24HAgo    float64  `json:"baseFees24hAgo"`
	QuoteFees         float64  `json:"quoteFees"`
	QuoteFees24HAgo   float64  `json:"quoteFees24hAgo"`
	FeeRate           float64  `json:"feeRate"`
	InitTime          int      `json:"initTime"`
	LatestTime        int      `json:"latestTime"`
	Events            int      `json:"events"`
}

type PoolTx struct {
	User types.EthAddress `json:"user"`
}

func FetchAllPoolStats(indexerEndpoint string, chainId types.ChainId) (result []PoolStats, provenance string, err error) {
	url, err := url.Parse(indexerEndpoint)
	query := url.Query()
	query.Add("chainId", string(chainId))
	query.Add("with24hPrices", "true")
	url.RawQuery = query.Encode()
	url.Path += "/all_pool_stats"
	// log.Println("Fetching all pool stats:", url.String())
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Indexer connection error: " + err.Error())
		return nil, "", err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Indexer read error: " + err.Error())
		return nil, "", err
	}
	type IndexerResp struct {
		Data       []PoolStats `json:"data"`
		Provenance struct {
			Hostname string `json:"hostname"`
		} `json:"provenance"`
	}
	response := IndexerResp{}
	err = json.Unmarshal(body, &response)
	return response.Data, response.Provenance.Hostname, err
}

func GetPoolTxs(indexerEndpoint string, pool PoolLoc) (result []PoolTx, err error) {
	url, err := url.Parse(indexerEndpoint)
	query := url.Query()
	query.Add("base", string(pool.Base))
	query.Add("quote", string(pool.Quote))
	query.Add("poolIdx", strconv.Itoa(pool.PoolIdx))
	query.Add("chainId", string(pool.ChainId))
	query.Add("n", "200")
	url.RawQuery = query.Encode()
	url.Path += "/pool_txs"
	log.Println("Fetching pool TXs:", url.String())
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Indexer connection error: " + err.Error())
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Indexer read error: " + err.Error())
		return nil, err
	}
	type IndexerResp struct {
		Data []PoolTx `json:"data"`
	}
	response := IndexerResp{}
	err = json.Unmarshal(body, &response)
	return response.Data, err
}
