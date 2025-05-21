package loader

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"fmt"

	"github.com/CrocSwap/analytics-server-go/types"
)

var vaultChains = []types.ChainId{"", "0x1", "0x82750", "0x783", "0x18232"}

const VAULT_REFRESH_PERIOD = 30 * time.Second

type VaultsWorker struct {
	vaults        map[types.ChainId][]byte
	vaultsUpdates map[types.ChainId]int64
}

func (s *VaultsWorker) RunVaultsWorker() {
	for {
		log.Println("Refreshing vaults")
		for _, chain := range vaultChains {
			for retry := range 5 {
				chainVaults, err := FetchChainVaults(chain)
				if err != nil {
					if chain == "98866" { // no plume yet, but it can still time out
						log.Printf("Skipping chain \"%s\"", chain)
						break
					}
					if retry > 2 { // noisy
						log.Printf("Error fetching vaults for chain \"%s\": %s", chain, err)
					}
					time.Sleep(5 * time.Second * time.Duration(retry))
					continue
				}
				log.Printf("Got vaults for chain \"%s\"", chain)
				s.vaults[chain] = chainVaults
				s.vaultsUpdates[chain] = time.Now().Unix()
				break
			}
		}
		time.Sleep(VAULT_REFRESH_PERIOD)
	}
}

func FetchChainVaults(chainId types.ChainId) (result []byte, err error) {
	url, err := url.Parse("https://protocol-service-api.tempestfinance.xyz/api/v1/vaults")
	query := url.Query()
	if chainId != "" {
		query.Add("chainId", fmt.Sprint(chainId.ToInt()))
	}
	url.RawQuery = query.Encode()
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)

	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("tempest API connection error: %w", err)
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("tempest API read error: %w", err)
		return nil, err
	}
	type TempestErrResp struct {
		Code          int             `json:"code"`
		Message       string          `json:"message"`
		ErrorEntities []string        `json:"errorEntities"`
		Details       json.RawMessage `json:"details"`
		Provenance    struct {
			Hostname string `json:"hostname"`
		} `json:"provenance"`
	}
	response := TempestErrResp{}
	err = json.Unmarshal(body, &response)
	if err != nil || response.Code != 0 {
		log.Println("Tempest API err: ", err, " resp: ", response)
		err = fmt.Errorf("tempest API error: %w", err)
		return nil, err
	}
	return body, err
}

func (l *Loader) GetVaults(optChainId types.ChainId) (vaults json.RawMessage, err error) {
	vaults, ok := l.vaultsWorker.vaults[optChainId]
	if ok {
		return
	}
	return nil, fmt.Errorf("vaults not found for chain \"%s\"", optChainId)
}
