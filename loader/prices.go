package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/utils"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

// If the price of any of these assets is less than 1% away from $1 then return $1.
// Users get confused when USD prices jump a tiny bit all the time.
var ONE_USD_STABLECOINS = []string{
	"0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4",
	"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
	"0x4300000000000000000000000000000000000003",
	"0xdac17f958d2ee523a2206206994597c13d831ec7",
	"0xf55bec9cafdbe8730f096aa55dad6d22d44099df",
	"0xca77eb3fefe3725dc33bccb54edefc3d9f764f97",
	"0x6b175474e89094c44da98b954eedeac495271d0f",
	"0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
	"0x78add880a697070c1e765ac44d65323a0dcce913",
	"0x3938a812c54304feffd266c7e2e70b48f9475ad6",
	"0xda6087e69c51e7d31b6dbad276a3c44703dfdcad",
	"0xa849026cda282eeebc3c39afcbe87a69424f16b4",
	"0xdddd73f5df1f0dc31373357beac77545dc5a6f3f",
	"0x6f2a1a886dbf8e36c4fa9f25a517861a930fbf3a",
	"0xeb466342c4d449bc9f53a865d5cb90586f405215",
}

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
	getPrice func(args PriceArgs, cacheKey string, ctx context.Context) (PriceValue, error)
	price    *PriceValue
}

const GET_PRICE_TIMEOUT = 2 * time.Second

func (l *Loader) GetPrice(args PriceArgs) (priceRespJson []byte, respCached bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetPrice panic: %v", r)
			return
		}
	}()
	args.AssetPlatform = strings.ToLower(args.AssetPlatform)
	args.TokenAddress = strings.ToLower(args.TokenAddress)
	normalArgs := l.fuzzyTokenLookup(args)

	// Since no price sources support these chains, return empty responses.
	if normalArgs.AssetPlatform == "plume" || normalArgs.AssetPlatform == "swell" || strings.HasPrefix(normalArgs.AssetPlatform, "monad") {
		priceResp := PriceResp{}
		priceRespJson, _ = json.Marshal(priceResp)
		return priceRespJson, true, nil
	}

	cacheKey := "price" + normalArgs.AssetPlatform + normalArgs.TokenAddress
	if cached, _, ok := l.GetFromCache(cacheKey); ok {
		resp := PriceResp{}
		_ = json.Unmarshal(cached, &resp)
		resp.Value.TokenAddress = args.TokenAddress // de-normalize the address
		newResp, _ := json.Marshal(resp)
		return newResp, true, nil
	}

	// Ordered by priority. CoinGecko prices are more reliable.
	priceSources := []PriceSource{
		{
			name:     "CoinGecko",
			getPrice: l.fetchCoinGeckoPrice,
			price:    &PriceValue{TokenAddress: args.TokenAddress, PriceSource: "1"},
		},
		{
			name:     "Llama",
			getPrice: l.fetchLlamaPrice,
			price:    &PriceValue{TokenAddress: args.TokenAddress, PriceSource: "2"},
		},
		{
			name:     "DEXScreener",
			getPrice: l.fetchDexScreenerPrice,
			price:    &PriceValue{TokenAddress: args.TokenAddress, PriceSource: "3"},
		},
	}

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, source := range priceSources {
		wg.Add(1)
		go func(source PriceSource) {
			defer wg.Done()
			price, err := source.getPrice(normalArgs, cacheKey, ctx)
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
			if slices.Index(ONE_USD_STABLECOINS, args.TokenAddress) != -1 && math.Abs(priceValue.UsdPrice-1) < 0.01 {
				priceValue.UsdPrice = 1
			}
			break
		}
	}

	if priceValue.UsdPrice > 10e7 {
		priceValue.UsdPrice = 0
		priceValue.PriceSource = "0"
	}

	if args.TokenAddress == "0xd294412741ee08aa3a35ac179ff0b4d9d7fefb27" { // fake SCR
		priceValue.UsdPrice = 0.0000000000001
	}

	priceResp := PriceResp{
		Value: priceValue,
	}
	priceRespJson, err = json.Marshal(priceResp)
	if err != nil {
		return nil, false, err
	}

	// since price requests aren't batched, it's better to spread out the cache TTL to smooth out bursts
	cache_ttl := PRICE_CACHE_TTL + time.Second*time.Duration(rand.Intn(40))
	l.AddToCache(cacheKey, priceRespJson, cache_ttl)
	log.Printf("Cached price for %v: %v", args, priceValue)
	return priceRespJson, false, nil
}

type coinGeckoResponse struct {
	Usd float64 `json:"usd"`
}

type llamaResponse struct {
	Coins map[string]llamaCoinPrice `json:"coins"`
}

type llamaCoinPrice struct {
	Price      float64 `json:"price"`
	Timestamp  int64   `json:"timestamp"`
	Confidence float64 `json:"confidence"`
}

// Most coins aren't on coingecko, so they don't need to be updated often.
const COINGECKO_NO_PRICE_CACHE_TTL = 4 * time.Hour

func (l *Loader) fetchCoinGeckoPrice(args PriceArgs, cacheKey string, ctx context.Context) (price PriceValue, err error) {
	if _, _, ok := l.GetFromCache("coingecko_missing" + cacheKey); ok {
		return
	}
	log.Println("CoinGecko price fetch", args)

	urlString := "https://pro-api.coingecko.com/api/v3"
	params := map[string]string{
		"vs_currencies": "usd",
		"precision":     "full",
	}
	tokenId := args.TokenAddress
	if args.TokenAddress == ZERO_ADDRESS && !strings.HasPrefix(args.AssetPlatform, "monad") {
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
	resp, err := l.httpRequest("GET", urlString, nil, headers, 3, ctx)

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

const LLAMA_MIN_CONFIDENCE = 0.5
const LLAMA_NO_PRICE_CACHE_TTL = 5 * time.Minute

func (l *Loader) fetchLlamaPrice(args PriceArgs, cacheKey string, ctx context.Context) (price PriceValue, err error) {
	if _, _, ok := l.GetFromCache("llama_missing" + cacheKey); ok {
		return
	}
	log.Println("Llama price fetch", args)

	tokenId := fmt.Sprintf("%s:%s", args.AssetPlatform, args.TokenAddress)
	urlString := fmt.Sprintf("https://coins.llama.fi/prices/current/%s?searchWidth=1h", tokenId)

	headers := map[string]string{"accept": "application/json"}
	resp, err := l.httpRequest("GET", urlString, nil, headers, 3, ctx)

	if err != nil {
		return
	}

	var llamaResp llamaResponse
	err = json.Unmarshal(resp, &llamaResp)
	if err != nil {
		return
	}

	if _, ok := llamaResp.Coins[tokenId]; !ok {
		l.AddToCache("llama_missing"+cacheKey, []byte{1}, LLAMA_NO_PRICE_CACHE_TTL)
		return price, errors.New("token not found in response")
	}
	if llamaResp.Coins[tokenId].Confidence < LLAMA_MIN_CONFIDENCE {
		return price, nil
	}
	price.UsdPrice = llamaResp.Coins[tokenId].Price
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
		BaseToken struct {
			Address string `json:"address"`
			Name    string `json:"name"`
			Symbol  string `json:"symbol"`
		} `json:"baseToken"`
		QuoteToken struct {
			Address string `json:"address"`
			Name    string `json:"name"`
			Symbol  string `json:"symbol"`
		} `json:"quoteToken"`
	} `json:"pairs"`
}

func (l *Loader) fetchDexScreenerPrice(args PriceArgs, cacheKey string, ctx context.Context) (price PriceValue, err error) {
	tokenAddress := args.TokenAddress
	if tokenAddress == ZERO_ADDRESS && !strings.HasPrefix(args.AssetPlatform, "monad") {
		tokenAddress = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
	}
	log.Println("DexScreener price fetch", args)
	urlString := "https://api.dexscreener.com/latest/dex/tokens/" + tokenAddress
	resp, err := l.httpRequest("GET", urlString, nil, nil, 2, ctx)

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

func (l *Loader) fuzzyTokenLookup(args PriceArgs) PriceArgs {
	// Sometimes tokens get sent with the wrong platform, so this is temporary for
	// for the most common tokens
	switch args.TokenAddress {
	case "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48":
		args.AssetPlatform = "ethereum"
	case "0xdac17f958d2ee523a2206206994597c13d831ec7":
		args.AssetPlatform = "ethereum"
	case "0x6b175474e89094c44da98b954eedeac495271d0f":
		args.AssetPlatform = "ethereum"
	case "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4":
		args.AssetPlatform = "scroll"
	case "0xf55bec9cafdbe8730f096aa55dad6d22d44099df":
		args.AssetPlatform = "scroll"
	case "0xa25b25548b4c98b0c7d3d27dca5d5ca743d68b7f":
		args.AssetPlatform = "scroll"
	case "0x3c1bca5a656e69edcd0d4e36bebb3fcdaca60cf1":
		args.AssetPlatform = "scroll"
	case "0x01f0a31698c4d065659b9bdc21b3610292a1c506":
		args.AssetPlatform = "scroll"
	case "0x4300000000000000000000000000000000000003":
		args.AssetPlatform = "blast"
	case "0xb1a5700fa2358173fe465e6ea4ff52e36e88e2ad":
		args.AssetPlatform = "blast"
	case "0x04c0599ae5a44757c0af6f9ec3b93da8976c150a":
		args.AssetPlatform = "blast"
	case "0xe7903b1f75c534dd8159b313d92cdcfbc62cb3cd":
		args.AssetPlatform = "blast"
	case "0x2416092f143378750bb29b79ed961ab195cceea5":
		args.AssetPlatform = "blast"
	}

	switch args.AssetPlatform {
	case "scroll":
		switch args.TokenAddress {
		case ZERO_ADDRESS:
			args.AssetPlatform = "ethereum"
		case "0xa25b25548b4c98b0c7d3d27dca5d5ca743d68b7f": // wrsETH
			args.TokenAddress = "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7"
			args.AssetPlatform = "ethereum"
		case "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4": // USDC
			args.TokenAddress = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
			args.AssetPlatform = "ethereum"
		case "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34": // USDe
			args.TokenAddress = "0x4c9edd5852cd905f086c759e8383e09bff1e68b3"
			args.AssetPlatform = "ethereum"
		case "0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2": // sUSDe
			args.TokenAddress = "0x9d39a5de30e57443bff2a8307a4256c8797a3497"
			args.AssetPlatform = "ethereum"
		case "0x5300000000000000000000000000000000000004": // WETH
			args.TokenAddress = "0x0000000000000000000000000000000000000000"
			args.AssetPlatform = "ethereum"
		case "0x89f17ab70cafb1468d633056161573efefea0713": // rswETH
			args.TokenAddress = "0xfae103dc9cf190ed75350761e95403b7b8afa6c0"
			args.AssetPlatform = "ethereum"
		default: // maybe bridged token
			counterpart := l.getScrollCounterpart(args.TokenAddress)
			if counterpart != "" && counterpart != ZERO_ADDRESS {
				// log.Println("Found scroll counterpart", counterpart, "for", args.TokenAddress)
				args.TokenAddress = counterpart
				args.AssetPlatform = "ethereum"
			}
		}
	case "blast":
		switch args.TokenAddress {
		case ZERO_ADDRESS:
			args.AssetPlatform = "ethereum"
		case "0x4300000000000000000000000000000000000004": // WETH
			args.TokenAddress = "0x0000000000000000000000000000000000000000"
			args.AssetPlatform = "ethereum"
		case "0xe7903b1f75c534dd8159b313d92cdcfbc62cb3cd": // wrsETH
			args.TokenAddress = "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7"
			args.AssetPlatform = "ethereum"
		case "0x2416092f143378750bb29b79ed961ab195cceea5": // ezETH
			args.TokenAddress = "0xbf5495efe5db9ce00f80364c8b423567e58d2110"
			args.AssetPlatform = "ethereum"
		}
	case "swell":
		switch args.TokenAddress {
		case ZERO_ADDRESS:
			args.AssetPlatform = "ethereum"
		case "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34": // USDe
			args.TokenAddress = "0x4c9edd5852cd905f086c759e8383e09bff1e68b3"
			args.AssetPlatform = "ethereum"
		case "0x18d33689ae5d02649a859a1cf16c9f0563975258": // rswETH
			args.TokenAddress = "0xfae103dc9cf190ed75350761e95403b7b8afa6c0"
			args.AssetPlatform = "ethereum"
		case "0xc3eacf0612346366db554c991d7858716db09f58": // rsETH
			args.TokenAddress = "0xa1290d69c65a6fe4df752f95823fae25cb99e5a7"
			args.AssetPlatform = "ethereum"
		case "0x09341022ea237a4db1644de7ccf8fa0e489d85b7": // swETH
			args.TokenAddress = "0xf951e335afb289353dc249e82926178eac7ded78"
			args.AssetPlatform = "ethereum"
		case "0xa6cb988942610f6731e664379d15ffcfbf282b44": // weETH
			args.TokenAddress = "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"
			args.AssetPlatform = "ethereum"
		case "0x2416092f143378750bb29b79ed961ab195cceea5": // ezETH
			args.TokenAddress = "0xbf5495efe5db9ce00f80364c8b423567e58d2110"
			args.AssetPlatform = "ethereum"
		case "0x9cb41cd74d01ae4b4f640ec40f7a60ca1bcf83e7": // pzETH
			args.TokenAddress = "0x8c9532a60e0e7c6bbd2b2c1303f63ace1c3e9811"
			args.AssetPlatform = "ethereum"
		case "0x2826d136f5630ada89c1678b64a61620aab77aea": // SWELL
			args.TokenAddress = "0x0a6e7ba5042b38349e437ec6db6214aec7b35676"
			args.AssetPlatform = "ethereum"
		case "0x58538e6a46e07434d7e7375bc268d3cb839c0133": // ENA
			args.TokenAddress = "0x57e114b691db790c35207b2e685d4a43181e6061"
			args.AssetPlatform = "ethereum"
		}
	case "plume":
		switch args.TokenAddress {
		case ZERO_ADDRESS: // PLUME
			fallthrough
		case "0xea237441c92cae6fc17caaf9a7acb3f953be4bd1": // WPLUME
			args.TokenAddress = "0x4c1746a800d224393fe2470c70a35717ed4ea5f1"
			args.AssetPlatform = "ethereum"
		case "0x78add880a697070c1e765ac44d65323a0dcce913": // USDC.e
			args.TokenAddress = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
			args.AssetPlatform = "ethereum"
		case "0x3938a812c54304feffd266c7e2e70b48f9475ad6": // USDC.e (Legacy)
			args.TokenAddress = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
			args.AssetPlatform = "ethereum"
		case "0xda6087e69c51e7d31b6dbad276a3c44703dfdcad": // USDT
			args.TokenAddress = "0xdac17f958d2ee523a2206206994597c13d831ec7"
			args.AssetPlatform = "ethereum"
		case "0xa849026cda282eeebc3c39afcbe87a69424f16b4": // USDT (Legacy)
			args.TokenAddress = "0xdac17f958d2ee523a2206206994597c13d831ec7"
			args.AssetPlatform = "ethereum"
		case "0xca59ca09e5602fae8b629dee83ffa819741f14be": // WETH
			args.TokenAddress = ZERO_ADDRESS
			args.AssetPlatform = "ethereum"
		case "0x39d1f90ef89c52dda276194e9a832b484ee45574": // pETH
			args.TokenAddress = ZERO_ADDRESS // TODO: change once there are price sources
			args.AssetPlatform = "ethereum"
		case "0xd630fb6a07c9c723cf709d2daa9b63325d0e0b73": // pETH (legacy)
			args.TokenAddress = ZERO_ADDRESS
			args.AssetPlatform = "ethereum"
		case "0xdddd73f5df1f0dc31373357beac77545dc5a6f3f": // pUSD
			args.TokenAddress = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" // TODO: change once there are price sources
			args.AssetPlatform = "ethereum"
		}
	case "monadTestnet":
		fallthrough
	case "monad":
		switch args.TokenAddress {
		case ZERO_ADDRESS:
			// TODO: add MON price after mainnet
		}
	}
	return args
}

const RPC_MAX_RETRIES = 2

func (l *Loader) getScrollCounterpart(tokenAddress string) (counterpart string) {
	counterpartBytes, _, ok := l.GetFromCache("scroll_counterpart_" + tokenAddress)
	if ok && counterpartBytes != nil {
		return string(counterpartBytes)
	}
	log.Printf("Getting scroll counterpart for %s", tokenAddress)

	ethClient := l.ethClients[534352]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addr := common.HexToAddress(tokenAddress)
	msg := ethereum.CallMsg{
		To:   &addr,
		Data: []byte{0x79, 0x75, 0x94, 0xb0},
	}
	for i := 0; i < RPC_MAX_RETRIES; i++ {
		resp, err := ethClient.CallContract(ctx, msg, nil)
		if err != nil {
			if strings.Contains(err.Error(), "execution reverted") {
				counterpart = ZERO_ADDRESS
				break
			}
			log.Printf("Error getting scroll counterpart: %v", err)
			time.Sleep(time.Second * time.Duration(i))
			continue
		}
		counterpart = strings.ToLower(common.BytesToAddress(resp).Hex())
		break
	}
	ttl := INFINITE_CACHE_TTL
	if len(counterpart) == 0 || counterpart == ZERO_ADDRESS { // if there was an error, retry soon
		ttl = 30 * time.Minute
	}
	l.AddToCache("scroll_counterpart_"+tokenAddress, []byte(counterpart), ttl)
	log.Printf("Cached scroll counterpart for %s: %s", tokenAddress, counterpart)
	return
}

func (l *Loader) httpRequest(method string, url string, body []byte, headers map[string]string, attempts int, ctx context.Context) (respBody []byte, err error) {

	for i := 0; i < attempts; i++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
		if err != nil {
			return nil, err
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := l.httpClient.Do(req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, err
			}
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
