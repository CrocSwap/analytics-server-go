package loader

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/CrocSwap/analytics-server-go/types"
)

type ChainConfig struct {
	NetworkName         string
	ChainID             int    `json:"chain_id"`
	RPCEndpoint         string `json:"rpc"`
	Subgraph            string `json:"subgraph"`
	Graphcache          string `json:"graphcache"`
	QueryContract       string `json:"query_contract"`
	QueryContractABI    string `json:"query_contract_abi"`
	KnockoutTickWidth   int    `json:"knockout_tick_width"`
	MulticallDisabled   bool   `json:"multicall_disabled"`
	MulticallContract   string `json:"multicall_contract"`
	MulticallMaxBatch   int    `json:"multicall_max_batch"`
	MulticallIntervalMs int    `json:"multicall_interval_ms"`
}

type NetworkConfig map[types.NetworkName]ChainConfig

func LoadNetworkConfig(path string) NetworkConfig {
	jsonData, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	var config NetworkConfig

	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		log.Fatal(err)
	}

	for chainName, chainCfg := range config {
		chainCfg.envVarOverride(chainName)
		chainCfg.NetworkName = string(chainName)
		config[chainName] = chainCfg
	}

	return config
}

func (c *NetworkConfig) ChainConfig(chainId types.ChainId) (ChainConfig, bool) {
	netName, isValid := c.NetworkForChainID(chainId)
	if isValid {
		cfg, hasCfg := (*c)[netName]
		if hasCfg {
			cfg.envVarOverride(netName)
			return cfg, true
		}
	}
	return ChainConfig{}, false
}

func (c *ChainConfig) envVarOverride(netName types.NetworkName) {
	envVar := "RPC_" + strings.ToUpper(string(netName))
	envVal := os.Getenv(envVar)
	if envVal != "" {
		c.RPCEndpoint = envVal
	}

	envVar2 := strings.ToUpper(string(netName)) + "_INFURA_KEY"
	envVal2 := os.Getenv(envVar2)
	if envVal == "" && envVal2 != "" {
		c.RPCEndpoint = envVal2
	}

	envVar = "SUBGRAPH_" + strings.ToUpper(string(netName))
	envVal = os.Getenv(envVar)
	if envVal != "" {
		c.Subgraph = envVal
	}
}

func (c *NetworkConfig) NetworkForChainID(chainId types.ChainId) (types.NetworkName, bool) {
	for networkKey, configElem := range *c {
		if chainId == types.IntToChainId(configElem.ChainID) {
			return networkKey, true
		}
	}
	return "", false
}

func (c *NetworkConfig) ChainIDForNetwork(network types.NetworkName) (types.ChainId, bool) {
	lookup, ok := (*c)[network]
	if ok {
		return types.IntToChainId(lookup.ChainID), true
	} else {
		return "", false
	}
}

func (c *NetworkConfig) RequireChainID(network types.NetworkName) types.ChainId {
	lookup, ok := (*c)[network]
	if !ok {
		log.Fatalf("No chainID found for %s", network)
	}
	return types.IntToChainId(lookup.ChainID)
}
