package loader

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/utils"
)

type PriceArgs struct {
	AssetPlatform string `json:"asset_platform"`
	TokenAddress  string `json:"token_address"`
}

type PriceResp struct {
	Value PriceValue `json:"value"`
}

type PriceValue struct {
	UsdPrice float64 `json:"usdPrice"`
	// UsdPriceLiqWeighted float64 `json:"usdPriceLiqWeighted"`
	// UsdPriceTopLiquid   float64 `json:"usdPriceTopLiquid"`
	// UsdPriceMedian      float64 `json:"usdPriceMedian"`
	TokenAddress string `json:"tokenAddress"`
	PriceSource  string `json:"source"`
	err          error  // for the channel only
}

type PriceSource struct {
	name     string
	getPrice func(args PriceArgs, cacheKey string) (PriceValue, error)
	price    *PriceValue
}

const GET_PRICE_TIMEOUT = 2 * time.Second

func (l *Loader) GetPrice(args PriceArgs) (priceRespBytes []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetPrice panic: %v", r)
			return
		}
	}()
	args.AssetPlatform = strings.ToLower(args.AssetPlatform)
	args.TokenAddress = strings.ToLower(args.TokenAddress)
	cacheKey := "price" + args.AssetPlatform + args.TokenAddress
	if cached, ok := l.GetFromCache(cacheKey); ok {
		return cached, nil
	}

	// Ordered by priority. CoinGecko prices are more reliable.
	priceSources := []PriceSource{
		{
			name:     "CoinGecko",
			getPrice: l.fetchCoinGeckoPrice,
			price:    &PriceValue{TokenAddress: args.TokenAddress, PriceSource: "1"},
		},
		{
			name:     "DEXScreener",
			getPrice: l.fetchDexScreenerPrice,
			price:    &PriceValue{TokenAddress: args.TokenAddress, PriceSource: "2"},
		},
	}

	wg := sync.WaitGroup{}
	for _, source := range priceSources {
		wg.Add(1)
		go func(source PriceSource) {
			defer wg.Done()
			price, err := source.getPrice(args, cacheKey)
			if err != nil {
				price.err = err
			}
			source.price.UsdPrice = price.UsdPrice
		}(source)
	}

	utils.WgWaitTimeout(&wg, GET_PRICE_TIMEOUT)

	priceValue := PriceValue{
		TokenAddress: args.TokenAddress,
	}
	for _, source := range priceSources {
		if source.price.err == nil && source.price.UsdPrice > 0 {
			priceValue = *source.price
			break
		}
	}

	priceResp := PriceResp{
		Value: priceValue,
	}
	priceRespBytes, err = json.Marshal(priceResp)
	if err != nil {
		return nil, err
	}

	// since price requests aren't batched, it's better to spread out the cache TTL to smooth out bursts
	cache_ttl := PRICE_CACHE_TTL + time.Second*time.Duration(rand.Intn(20))
	l.AddToCache(cacheKey, priceRespBytes, cache_ttl)
	return
}

type coinGeckoResponse struct {
	Usd float64 `json:"usd"`
}

// Most coins aren't on coingecko, so they don't need to be updated often.
const COINGECKO_NO_PRICE_CACHE_TTL = 8 * time.Hour

func (l *Loader) fetchCoinGeckoPrice(args PriceArgs, cacheKey string) (price PriceValue, err error) {
	log.Println("CoinGecko price fetch", args)
	price.TokenAddress = args.TokenAddress
	if _, ok := l.GetFromCache("coingecko_missing" + cacheKey); ok {
		return
	}

	urlString := "https://pro-api.coingecko.com/api/v3"
	params := map[string]string{
		"vs_currencies": "usd",
		"precision":     "full",
	}
	tokenId := args.TokenAddress
	if args.TokenAddress == "0x0000000000000000000000000000000000000000" {
		urlString += "/simple/price"
		tokenId = "ethereum"
		params["ids"] = tokenId
	} else {
		urlString += "/simple/token_price/" + args.AssetPlatform
		params["contract_addresses"] = tokenId
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	urlString = u.String()

	headers := map[string]string{"x-cg-pro-api-key": os.Getenv("COINGECKO_API_KEY"), "accept": "application/json"}
	resp, err := l.httpRequest("GET", urlString, nil, headers)

	if err != nil {
		return
	}

	var cgResp map[string]coinGeckoResponse
	err = json.Unmarshal(resp, &cgResp)
	if err != nil {
		return
	}

	if _, ok := cgResp[tokenId]; !ok {
		l.AddToCache("coingecko_missing"+cacheKey, []byte{1}, COINGECKO_NO_PRICE_CACHE_TTL)
		return price, errors.New("token not found in response")
	}
	price.UsdPrice = cgResp[tokenId].Usd
	return
}

type DexScreenerTokensResp struct {
	Pairs []struct {
		ChainID  string `json:"chainId"`
		DexID    string `json:"dexId"`
		PriceUsd string `json:"priceUsd"`
		Volume   struct {
			H24 float64 `json:"h24"`
			H6  float64 `json:"h6"`
			H1  float64 `json:"h1"`
			M5  float64 `json:"m5"`
		} `json:"volume"`
		PriceChange struct {
			M5  float64 `json:"m5"`
			H1  float64 `json:"h1"`
			H6  float64 `json:"h6"`
			H24 float64 `json:"h24"`
		} `json:"priceChange"`
		Liquidity struct {
			Usd   float64 `json:"usd"`
			Base  float64 `json:"base"`
			Quote float64 `json:"quote"`
		} `json:"liquidity"`
	} `json:"pairs"`
}

func (l *Loader) fetchDexScreenerPrice(args PriceArgs, cacheKey string) (price PriceValue, err error) {
	tokenAddress := args.TokenAddress
	if tokenAddress == "0x0000000000000000000000000000000000000000" {
		tokenAddress = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
	}
	log.Println("DexScreener price fetch", args)
	urlString := "https://api.dexscreener.com/latest/dex/tokens/" + tokenAddress
	resp, err := l.httpRequest("GET", urlString, nil, nil)

	if err != nil {
		return
	}

	var dsResp DexScreenerTokensResp
	// log.Println("ds resp", string(resp))
	err = json.Unmarshal(resp, &dsResp)
	// log.Println("ds err", err)
	if err != nil {
		return
	}

	price = getDexScreenerPrice(&dsResp, tokenAddress)
	return
}

const MIN_LIQ_FOR_PRICE = 5000

const TAKE_TOP_N_DEXES = 1

// Calculate one price from multiple pools, some of which return completely wrong prices.
func getDexScreenerPrice(ds *DexScreenerTokensResp, tokenAddress string) (price PriceValue) {
	price.TokenAddress = tokenAddress
	for _, pair := range ds.Pairs {
		if pair.PriceUsd == "" {
			continue
		}
		priceFloat, err := strconv.ParseFloat(pair.PriceUsd, 64)
		if err != nil {
			log.Printf("Error parsing price for %s: %s : %v", tokenAddress, pair.PriceUsd, pair)
			continue
		}
		if pair.Liquidity.Usd < MIN_LIQ_FOR_PRICE {
			continue
		}
		price.UsdPrice = priceFloat
		break
	}
	return

	// All of the code below only makes the resulting price worse. DEXScreener has absolutely random
	// prices between DEXes. There could be the top liquid pools with a wrong price, there could be
	// the majority of pools with a wrong price, there could be pools with both wrong liquidity and
	// price. No idea how you could calculate a correct price from this mess.

	// type liqPrice struct {
	// 	priceUsd float64
	// 	liqUsd   float64
	// 	dexId    string
	// }

	// liqSum := 0.0
	// topLiqPrice := liqPrice{}
	// liqPrices := []liqPrice{}
	// for _, pair := range ds.Pairs {
	// 	if pair.PriceUsd == "" {
	// 		continue
	// 	}
	// 	price, err := strconv.ParseFloat(pair.PriceUsd, 64)
	// 	if err != nil {
	// 		log.Printf("Error parsing price for %s: %s : %v", tokenAddress, pair.PriceUsd, pair)
	// 		continue
	// 	}
	// 	if pair.Liquidity.Usd < MIN_LIQ_FOR_PRICE {
	// 		continue
	// 	}
	// 	liqPrices = append(liqPrices, liqPrice{
	// 		priceUsd: price,
	// 		liqUsd:   pair.Liquidity.Usd,
	// 		dexId:    pair.DexID,
	// 	})
	// 	liqSum += pair.Liquidity.Usd
	// 	if pair.Liquidity.Usd > topLiqPrice.liqUsd {
	// 		topLiqPrice.priceUsd = price
	// 		topLiqPrice.liqUsd = pair.Liquidity.Usd
	// 		topLiqPrice.dexId = pair.DexID
	// 	}
	// 	if len(liqPrices) >= TAKE_TOP_N_DEXES {
	// 		break
	// 	}
	// }

	// // log.Println("liqPrices", liqPrices)

	// liqWeightedPrice := 0.0
	// for _, lp := range liqPrices {
	// 	liqWeightedPrice += lp.priceUsd * (lp.liqUsd / liqSum)
	// }
	// // log.Println("liqWeightedPrice", liqWeightedPrice)

	// slices.SortFunc(liqPrices, func(i, j liqPrice) int {
	// 	if i.priceUsd < j.priceUsd {
	// 		return -1
	// 	} else if i.priceUsd > j.priceUsd {
	// 		return 1
	// 	} else {
	// 		return 0
	// 	}
	// })

	// medianPrice := 0.0
	// if len(liqPrices) > 0 {
	// 	if len(liqPrices)%2 == 0 {
	// 		medianPrice = (liqPrices[len(liqPrices)/2].priceUsd + liqPrices[len(liqPrices)/2-1].priceUsd) / 2
	// 	} else {
	// 		medianPrice = liqPrices[len(liqPrices)/2].priceUsd
	// 	}
	// }

	// return PriceValue{
	// 	UsdPriceLiqWeighted: liqWeightedPrice,
	// 	UsdPriceTopLiquid:   topLiqPrice.priceUsd,
	// 	UsdPriceMedian:      medianPrice,
	// 	TokenAddress:        tokenAddress,
	// }
}

const HTTP_MAX_RETRIES = 5

func (l *Loader) httpRequest(method string, url string, body []byte, headers map[string]string) (respBody []byte, err error) {

	for i := 0; i < HTTP_MAX_RETRIES; i++ {
		req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
		if err != nil {
			return nil, err
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := l.httpClient.Do(req)
		if err != nil {
			log.Printf("Error making request to: \"%s\", retrying: %s", url, err)
			time.Sleep(time.Second*time.Duration(i) + time.Millisecond*time.Duration(rand.Intn(1000)))
			continue
		}

		defer resp.Body.Close()
		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response body for \"%s\", retrying: %s", url, err)
			time.Sleep(time.Second*time.Duration(i) + time.Millisecond*time.Duration(rand.Intn(1000)))
			continue
		}

		if resp.StatusCode != 200 {
			log.Printf("Error response from %s: %s", url, respBody)
			time.Sleep(time.Second*time.Duration(i) + time.Millisecond*time.Duration(rand.Intn(1000)))
			continue
		}

		return respBody, nil
	}
	return
}
