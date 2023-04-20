package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const numberOfHashForMultiHash = 6

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	waitGroup := &sync.WaitGroup{}
	for _, jobWork := range jobs {
		waitGroup.Add(1)
		out := make(chan interface{})
		go payloadWorker(in, out, jobWork, waitGroup)
		in = out
	}
	waitGroup.Wait()
}

func payloadWorker(in chan interface{}, out chan interface{}, jobWork job, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	defer close(out)
	jobWork(in, out)
}

func SingleHash(in chan interface{}, out chan interface{}) {
	mutex := &sync.Mutex{}
	waitGroup := &sync.WaitGroup{}

	for wrapDataPipeline := range in {
		waitGroup.Add(1)
		unwrapDataPipeline := fmt.Sprintf("%v", wrapDataPipeline)
		go calcSingleHash(unwrapDataPipeline, waitGroup, mutex, out)
	}

	waitGroup.Wait()
}

func calcSingleHash(unwrapDataPipeline string, waitGroup *sync.WaitGroup, mutex *sync.Mutex, out chan interface{}) {
	defer waitGroup.Done()

	mutex.Lock()
	data := fmt.Sprintf("%v", unwrapDataPipeline)
	fmt.Println("Hello " + unwrapDataPipeline)
	hashMD5 := routineMD5(data)
	mutex.Unlock()

	data = singleHashParallelsCalcCrc32(data, hashMD5)
	out <- data
}

func routineMD5(data string) string {
	data = DataSignerMd5(data)
	fmt.Println("SingleHash md5(data) " + data)
	return data
}

func singleHashParallelsCalcCrc32(data string, hashMD5 string) string {
	waitGroup := &sync.WaitGroup{}
	inData := [2]string{data, hashMD5}
	var outData [2]string

	for i := 0; i < 2; i++ {
		waitGroup.Add(1)
		go func(i int) {
			outData[i] = DataSignerCrc32(inData[i])
			defer waitGroup.Done()
		}(i)
	}

	waitGroup.Wait()

	fmt.Println("SingleHash crc32(md5(data)) " + outData[1])
	fmt.Println("SingleHash crc32(data) " + outData[0])
	return outData[0] + "~" + outData[1]
}

func MultiHash(in chan interface{}, out chan interface{}) {
	waitGroup := &sync.WaitGroup{}

	for wrapDataPipeline := range in {
		waitGroup.Add(1)
		unwrapDataPipeline := fmt.Sprintf("%v", wrapDataPipeline)
		go func() {
			calcMultiHash(unwrapDataPipeline, out)
			defer waitGroup.Done()
		}()
	}

	waitGroup.Wait()
}

func calcMultiHash(data string, channel chan interface{}) {
	var hashArray [numberOfHashForMultiHash]string
	waitGroup := &sync.WaitGroup{}

	for th := 0; th < numberOfHashForMultiHash; th++ {
		waitGroup.Add(1)
		go routineCrc32(data, th, &hashArray, waitGroup)
	}
	waitGroup.Wait()

	channel <- strings.Join(hashArray[:], "")
	fmt.Println(data + " MultiHash result: " + strings.Join(hashArray[:], ""))
}

func routineCrc32(data string, th int, hashArray *[6]string, waitGroup *sync.WaitGroup) {
	hashArray[th] = DataSignerCrc32(strconv.Itoa(th) + data)
	defer waitGroup.Done()
	fmt.Println(data + " MultiHash: crc32(th + step1) " + strconv.Itoa(th) + " " + hashArray[th])
}

func CombineResults(in, out chan interface{}) {
	var result []string

	for i := range in {
		result = append(result, i.(string))
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}
