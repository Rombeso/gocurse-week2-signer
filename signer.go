package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	in := make(chan interface{}, MaxInputDataLen)
	for _, jobFunc := range jobs {
		wg.Add(1)
		out := make(chan interface{}, MaxInputDataLen)
		go func(jobFunc job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jobFunc(in, out)
		}(jobFunc, in, out, wg)
		in = out
	}
}

func parallelProcessCrc32(data string, item chan string) {
	item <- DataSignerCrc32(data)
	close(item)
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	md5Mux := &sync.Mutex{}

	for val := range in {
		var chans []chan string
		var i int
		wg.Add(1)
		go func(val interface{}) {
			defer wg.Done()
			chans = append(chans, make(chan string, 1)) // crc32(data)
			go parallelProcessCrc32(fmt.Sprint(val), chans[i])
			md5Mux.Lock()
			md5 := DataSignerMd5(fmt.Sprint(val)) // md5(data)
			md5Mux.Unlock()
			chans = append(chans, make(chan string, 1))
			go parallelProcessCrc32(md5, chans[i+1]) // crc32(md5(data)
			crc32 := <-chans[i]
			crc32Md5 := <-chans[i+1]
			result := crc32 + "~" + crc32Md5
			out <- result
			i += 2
		}(val)
	}
	wg.Wait()
}
func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {

		wg.Add(1)
		go func(data string) {
			defer wg.Done()
			workers := &sync.WaitGroup{}
			dataHashes := make([]string, 6)
			for th := 0; th < 6; th++ {
				workers.Add(1)
				go func(th int) {
					defer workers.Done()
					dataHashes[th] = DataSignerCrc32(fmt.Sprint(th) + data)
				}(th)
			}
			workers.Wait()
			out <- strings.Join(dataHashes, "")
		}(fmt.Sprint(data))
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result []string
	for val := range in {
		result = append(result, fmt.Sprint(val))
	}
	sort.Strings(result)
	strRes := strings.Join(result, "_")
	out <- strRes
	fmt.Printf("CombineResults %v\n", strRes)
}

func main() {
	inputData := []int{0, 1}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
	}

	ExecutePipeline(hashSignJobs...)
}
