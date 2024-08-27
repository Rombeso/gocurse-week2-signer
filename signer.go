package main

import (
	"fmt"
	"sort"
	"strconv"
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

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	md5Mux := &sync.Mutex{}

	for val := range in {
		wg.Add(1)
		go func(val interface{}) {
			defer wg.Done()
			data := strconv.Itoa(val.(int))
			crc32Chan := make(chan string, 1)
			md5Chan := make(chan string, 1)

			go func() {
				crc32Chan <- DataSignerCrc32(data)
			}()

			go func() {
				md5Mux.Lock()
				md5Data := DataSignerMd5(data)
				md5Mux.Unlock()
				md5Chan <- DataSignerCrc32(md5Data)
			}()

			crc32 := <-crc32Chan
			crc32Md5 := <-md5Chan
			result := crc32 + "~" + crc32Md5
			out <- result
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
					dataHashes[th] = DataSignerCrc32(strconv.Itoa(th) + data)
				}(th)
			}
			workers.Wait()
			out <- strings.Join(dataHashes, "")
		}(data.(string))
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result []string
	for val := range in {
		result = append(result, val.(string))
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
