package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	addr            = flag.String("addr", "localhost:8080", "The HTTP host port for the instance that is benchmarked.")
	writeIterations = flag.Int("write-iterations", 1000, "The number of iterations for writing")
	readIterations  = flag.Int("read-iterations", 1000, "The number if iterations fir reading")
	concurrency     = flag.Int("concurrency", 1, "How many go routines to run in parallel for each number of iterations")
)

func benchmark(funcName string, iterations int, fn func() []byte) (float64, [][]byte) {
	var max time.Duration
	var min = time.Hour
	var keys [][]byte
	start := time.Now()
	for i := 0; i < iterations; i++ {
		iterStart := time.Now()
		keys = append(keys, fn())
		iterTime := time.Since(iterStart)

		if iterTime < min {
			min = iterTime
		}
		if iterTime > max {
			max = iterTime
		}
	}
	avg := time.Since(start) / time.Duration(iterations)
	qps := float64(iterations) / (float64(time.Since(start)) / float64(time.Second))
	fmt.Printf("Func %s took avg %s, QPS %.1f, max %s, min %s\n", funcName, avg, qps, max, min)
	return qps, keys
}

func writeRand() []byte {
	key := fmt.Sprintf("key-%d", rand.Intn(1000000))
	value := fmt.Sprintf("value-%d", rand.Intn(1000000))
	values := url.Values{}
	values.Set("key", key)
	values.Set("value", value)
	resp, err := http.Get("http://" + (*addr) + "/set?" + values.Encode())

	if err != nil {
		log.Fatalf("Error during set: %v", err)
	}

	defer resp.Body.Close()

	// fmt.Printf("%s = %s\n", key, value)
	return []byte(key)
}

func readRand(allKeys [][]byte) []byte {
	key := string(allKeys[rand.Intn(len(allKeys))])
	values := url.Values{}
	values.Set("key", key)
	resp, err := http.Get("http://" + (*addr) + "/get?" + values.Encode())
	if err != nil {
		log.Fatalf("Error during set: %v", err)
	}

	defer resp.Body.Close()

	// fmt.Printf("%s = %s\n", key, value)
	return []byte(key)
}

func main() {
	flag.Parse()
	fmt.Printf("Running with concurrency level %d\n", *concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalQps float64
	var allKeys [][]byte
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			qps, keys := benchmark("write", *writeIterations, writeRand)
			mu.Lock()
			allKeys = append(allKeys, keys...)
			totalQps += qps
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("Total write QPS %.1f, total set %d keys", totalQps, len(allKeys))

	totalQps = 0
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			qps, _ := benchmark("read", *readIterations, func() []byte {
				return readRand(allKeys)
			})
			mu.Lock()
			totalQps += qps
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("Total read QPS %.1f", totalQps)

}
