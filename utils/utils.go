package utils

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const RETRY_CALL_MIN_WAIT = 1
const RETRY_CALL_MAX_WAIT = 20
const N_MAX_RETRIES = 5

func retryWaitRandom() {
	waitTime := rand.Intn(RETRY_CALL_MAX_WAIT-RETRY_CALL_MIN_WAIT) + RETRY_CALL_MIN_WAIT
	time.Sleep(time.Duration(waitTime) * time.Second)
}

func TryFnAttempt[T any](fn func() (T, error), label string) (T, error) {
	result, err := fn()
	for retryCount := 0; err != nil && retryCount < N_MAX_RETRIES; retryCount += 1 {
		log.Printf("Call \"%s\" failed at attempt %d, sleeping and retrying", label, retryCount)
		retryWaitRandom()
		result, err = fn()
	}
	if err != nil {
		log.Println("Giving up call for:", label)
		return result, err
	}
	return result, nil
}

func WgWaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}
