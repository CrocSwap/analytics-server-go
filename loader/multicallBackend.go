package loader

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/CrocSwap/analytics-server-go/types"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Call3InputType struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Call3OutputType struct {
	Success    bool
	ReturnData []uint8
}

type MulticallResult struct {
	Success    bool
	Error      error
	ReturnData []byte
}

type CallJob struct {
	Contract types.EthAddress
	CallData []byte
	Result   chan MulticallResult
}

type BatchedEthClient struct {
	Cfg           ChainConfig
	codeCache     map[string][]byte
	codeCacheLock sync.RWMutex
	jobChan       chan CallJob
	multicallAbi  abi.ABI
	client        *ethclient.Client
}

const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

// CallContract implements bind.ContractBackend.
func (c *BatchedEthClient) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return c.contractDataCall(types.EthAddress(call.To.String()), call.Data, blockNumber)
}

// CodeAt implements bind.ContractBackend.
func (c *BatchedEthClient) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	contractStr := contract.String()
	if contractStr == ZERO_ADDRESS { // why would it do this?
		return []byte{}, nil
	}
	c.codeCacheLock.RLock()
	if code, ok := c.codeCache[contractStr]; ok {
		return code, nil
	}
	c.codeCacheLock.RUnlock()
	code, err := c.client.CodeAt(ctx, contract, blockNumber)
	if err != nil && err.Error() != "no contract code at given address" {
		return []byte{}, err
	}
	c.codeCacheLock.Lock()
	defer c.codeCacheLock.Unlock()
	c.codeCache[contractStr] = code
	return code, nil
}

func NewBatchedEthClient(chain ChainConfig) *BatchedEthClient {
	log.Println("NewBatchedEthClient", chain)
	client, err := ethclient.Dial(chain.RPCEndpoint)
	if err != nil {
		panic(err)
	}
	c := &BatchedEthClient{
		Cfg:          chain,
		multicallAbi: multicallAbi(),
		client:       client,
		codeCache:    make(map[string][]byte),
	}
	if !chain.MulticallDisabled && chain.MulticallContract != "" {
		c.jobChan = make(chan CallJob)
		go c.multicallWorker()
	}
	return c
}

func multicallAbi() abi.ABI {
	filePath := "./artifacts/abis/Multicall.json"
	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("Failed to read ABI contract at " + filePath)
	}

	parsedABI, err := abi.JSON(file)
	if err != nil {
		log.Fatalf("Failed to parse contract ABI: %v", err)
	}

	return parsedABI
}

const MULTICALL_TIMEOUT_MS = 5000

func (c *BatchedEthClient) contractDataCall(contract types.EthAddress, data []byte, blockNumber *big.Int) ([]byte, error) {
	if c.jobChan == nil || (blockNumber != nil && blockNumber != big.NewInt(0)) { // if multicall is disabled for this chain
		log.Println("Multicall disabled, calling manually")
		return c.singleContractDataCall(contract, data, blockNumber)
	}

	job := CallJob{
		Contract: contract,
		CallData: data,
		Result:   make(chan MulticallResult, 1), // buffered to not lock the worker if the call timed out
	}
	c.jobChan <- job

	// Wait for the multicall result and fall back to a direct call if it times out
	select {
	case result := <-job.Result:
		if !result.Success || result.Error != nil {
			log.Printf("Warning multicall success=%v, calling manually. Error: %v", result.Success, result.Error)
			return c.singleContractDataCall(contract, data, nil)
		}
		return result.ReturnData, nil
	case <-time.After(MULTICALL_TIMEOUT_MS * time.Millisecond):
		log.Println("Multicall timed out, calling manually")
		return c.singleContractDataCall(contract, data, nil)
	}
}

// Call a contract directly
func (c *BatchedEthClient) singleContractDataCall(contract types.EthAddress, data []byte, blockNumber *big.Int) ([]byte, error) {
	addr := common.HexToAddress(string(contract))

	msg := ethereum.CallMsg{
		To:   &addr,
		Data: data,
	}

	result, err := c.client.CallContract(context.Background(), msg, blockNumber)
	if err != nil {
		return []byte{}, err
	}
	return result, nil
}

// Goroutine that aggregates calls and sends them to the multicall contract after a timeout
func (c *BatchedEthClient) multicallWorker() {
	jobs := make([]CallJob, 0)
	batchTimer := time.NewTimer(1<<63 - 1) // infinite timer until the first job
	maxBatchSize := c.Cfg.MulticallMaxBatch
	if maxBatchSize == 0 {
		maxBatchSize = 10
	}
	batchInterval := time.Duration(c.Cfg.MulticallIntervalMs) * time.Millisecond
	if batchInterval == time.Duration(0) {
		batchInterval = time.Duration(500) * time.Millisecond
	}

	log.Println("multicallWorker started", c.Cfg.ChainID, "maxBatchSize", maxBatchSize, "batchInterval", batchInterval)

	for {
		// Timer starts as soon as the first job is received.
		// If the timer finishes or the batch is full, the batch is sent.
		select {
		case job := <-c.jobChan:
			if len(jobs) == 0 {
				batchTimer.Reset(batchInterval)
			}
			jobs = append(jobs, job)
			if len(jobs) < maxBatchSize {
				continue
			}
		case <-batchTimer.C:
		}
		batchTimer.Reset(1<<63 - 1)

		if len(jobs) == 1 {
			// Send a single call directly
			job := jobs[0]
			result, err := c.singleContractDataCall(job.Contract, job.CallData, nil)
			if err != nil {
				job.Result <- MulticallResult{Success: false, Error: err}
			} else {
				job.Result <- MulticallResult{Success: true, ReturnData: result}
			}
		} else {
			err := c.multicall(jobs)
			// Cancel all jobs if the multicall fails
			if err != nil {
				for _, job := range jobs {
					job.Result <- MulticallResult{Success: false, Error: err}
				}
			}
		}
		jobs = jobs[:0]
	}
}

// Sends a batch of calls to the multicall contract
func (c *BatchedEthClient) multicall(jobs []CallJob) (err error) {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Sprintln("multicall panic", err)
		}
	}()

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("Sending %d calls to multicall", len(jobs))
	inputs := make([]Call3InputType, len(jobs))
	for i, job := range jobs {
		input := Call3InputType{
			Target:       common.HexToAddress(string(job.Contract)),
			AllowFailure: true,
			CallData:     job.CallData,
		}
		inputs[i] = input
	}
	packed, err := c.multicallAbi.Pack("aggregate3", inputs)
	if err != nil {
		log.Println("failed to pack aggregate3", err)
		return err
	}

	multicallResult, err := c.singleContractDataCall(types.EthAddress(c.Cfg.MulticallContract), packed, nil)
	if err != nil {
		return err
	}

	var results []Call3OutputType
	err = c.multicallAbi.UnpackIntoInterface(&results, "aggregate3", multicallResult)
	if err != nil {
		log.Println("failed to unpack aggregate3", err)
		return err
	}

	for i, job := range jobs {
		result := results[i]
		if result.Success {
			job.Result <- MulticallResult{Success: true, ReturnData: result.ReturnData}
		} else {
			job.Result <- MulticallResult{Success: false}
		}
	}
	return nil
}

// Below are the unused methods to implement the bind.ContractBackend interface

func (c *BatchedEthClient) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]ethTypes.Log, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*ethTypes.Header, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) SendTransaction(ctx context.Context, tx *ethTypes.Transaction) error {
	panic("unimplemented")
}

func (c *BatchedEthClient) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- ethTypes.Log) (ethereum.Subscription, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	panic("unimplemented")
}

func (c *BatchedEthClient) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	panic("unimplemented")
}
