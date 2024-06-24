package loader

import (
	"encoding/json"
	"fmt"
	"log"
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

func (l *Loader) GetEns(address string) (ensRespBytes []byte, err error) {
	cacheKey := "ens" + address
	if cached, ok := l.GetFromCache(cacheKey); ok {
		return cached, nil
	}

	ensResp, err := l.fetchEns(address)
	if err != nil {
		return nil, err
	}

	ensRespBytes, err = json.Marshal(ensResp)
	if err != nil {
		return nil, err
	}

	l.AddToCache(cacheKey, ensRespBytes, ENS_CACHE_TTL)
	return
}

const ENS_MAX_RETRIES = 5

func (l *Loader) fetchEns(address string) (ensResp EnsResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fetchEns panic: %v", r)
			return
		}
	}()

	var reverse string
	for i := 0; i < ENS_MAX_RETRIES; i++ {
		reverse, err = ens.ReverseResolve(l.ethClients[1], common.HexToAddress(address))
		if err != nil && (err.Error() == "not a resolver" || err.Error() == "no resolution" || err.Error() == "no contract code at given address") {
			ensResp.Ens = nil
			return ensResp, nil
		}
		if err != nil {
			log.Printf("fetchEns error: %v", err)
			time.Sleep(time.Second * time.Duration(i))
			// time.Sleep(time.Second*time.Duration(i) + time.Millisecond*time.Duration(rand.Intn(1000)))
			continue
		}
		ensResp.Ens = &reverse
		return ensResp, nil
	}
	return
}
