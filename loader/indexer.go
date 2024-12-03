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
	ChainId string `json:"chainId"`
	Base    string `json:"base"`
	Quote   string `json:"quote"`
	PoolIdx int    `json:"poolIdx"`
}

type PoolStats struct {
	PoolLoc
	Events int `json:"events"`
}

type PoolTx struct {
	User string `json:"user"`
}

func GetAllPoolStats(indexerEndpoint string, chainId types.ChainId) (result []PoolStats, err error) {
	url, err := url.Parse(indexerEndpoint)
	query := url.Query()
	query.Add("chainId", string(chainId))
	url.RawQuery = query.Encode()
	url.Path += "/all_pool_stats"
	log.Println("Fetching all pool stats:", url.String())
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)
	req.Header.Set("Content-Type", "application/json")

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
		Data []PoolStats `json:"data"`
	}
	response := IndexerResp{}
	err = json.Unmarshal(body, &response)
	return response.Data, err
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
	req.Header.Set("Content-Type", "application/json")

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
