package loader

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ens "github.com/wealdtech/go-ens/v3"
)

type EnsArgs struct {
	Address string `json:"address"`
}

type EnsResp struct {
	Ens *string `json:"ens_address"`
}

func (l *Loader) GetEns(address string) (ensRespJson []byte, expiresAt time.Time, err error) {
	address = strings.ToLower(address)
	cacheKey := "ens" + address
	if cached, expiresAt, ok := l.GetFromCache(cacheKey); ok {
		return cached, expiresAt, nil
	}

	// Pre-cache empty response to avoid retrying too many times
	emptyEnsResp := EnsResp{Ens: nil}
	emptyEnsRespJson, _ := json.Marshal(emptyEnsResp)
	l.AddToCache(cacheKey, emptyEnsRespJson, 60*time.Second)
	expiresAt = time.Now().Add(60 * time.Second)

	var ensResp EnsResp
	ensResp, err = l.fetchEns(address)
	if err != nil {
		return
	}

	ensRespJson, err = json.Marshal(ensResp)
	if err != nil {
		return
	}

	l.AddToCache(cacheKey, ensRespJson, ENS_CACHE_TTL)
	expiresAt = time.Now().Add(ENS_CACHE_TTL)
	return
}

const ENS_MAX_RETRIES = 3

func (l *Loader) fetchEns(address string) (ensResp EnsResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fetchEns panic: %v", r)
			return
		}
	}()

	var domain string
	for i := range ENS_MAX_RETRIES {
		if i > 0 {
			log.Printf("fetchEns retry #%d for \"%s\", last error: \"%s\"", i, address, err)
		}
		domain, err = ens.ReverseResolve(l.ethClients[1], common.HexToAddress(address))
		if err != nil && (err.Error() == "not a resolver" || err.Error() == "no resolution" || err.Error() == "no contract code at given address") {
			ensResp.Ens = nil
			return ensResp, nil
		}
		if err != nil && err.Error() != "no address" {
			log.Printf("fetchEns reverse error for \"%s\": %v", address, err)
			time.Sleep(time.Second * time.Duration(i))
			// time.Sleep(time.Second*time.Duration(i) + time.Millisecond*time.Duration(rand.Intn(1000)))
			continue
		}
		if domain == ZERO_ADDRESS || (err != nil && err.Error() == "no address") {
			ensResp.Ens = nil
			return ensResp, nil
		}
		normDomain, _ := ens.NormaliseDomain(domain)
		forwardAddr, err := ens.Resolve(l.ethClients[1], normDomain)
		if err != nil && err.Error() != "unregistered name" && !strings.HasPrefix(err.Error(), "execution reverted") && err.Error() != "no address" && err.Error() != "no resolver" {
			log.Printf("fetchEns forward error for \"%s\" - \"%s\": %v", address, domain, err)
			time.Sleep(time.Second * time.Duration(i))
			continue
		}
		if err != nil {
			ensResp.Ens = nil
			return ensResp, nil
		}
		if strings.ToLower(forwardAddr.Hex()) == address {
			ensResp.Ens = &domain
		}
		return ensResp, nil
	}
	return
}
