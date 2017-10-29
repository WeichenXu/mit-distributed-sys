package main

import (
	"fmt"
	"sync"
)

// 1. Use mutex and wait group
// 2. Use golang channel

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

/* Serialized */
// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func CrawlSerialized(url string, depth int, fetcher Fetcher) {
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		CrawlSerialized(u, depth-1, fetcher)
	}
	return
}

/* Parallel with mutex*/
type fetchState struct {
	m_mutex  sync.Mutex
	m_states map[string]bool
}

func makeFetchState() *fetchState {
	f := &fetchState{}
	f.m_states = make(map[string]bool)
	return f
}

func (f *fetchState) checkState(url string) bool {
	//f.m_mutex.Lock()
	//defer f.m_mutex.Unlock()
	if f.m_states[url] {
		return true
	}
	f.m_states[url] = true
	return false
}

func CrawlMutex(url string, depth int, fetcher Fetcher, f *fetchState) {
	if depth < 0 || f.checkState(url) {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	var wg sync.WaitGroup
	for _, u := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			CrawlMutex(u, depth-1, fetcher, f)
		}(u)
	}
	wg.Wait()
	return
}

/* parallel with channels */
func doFetch(url string, fetcher Fetcher, ch chan []string) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		ch <- []string{}
	} else {
		fmt.Printf("found: %s %q\n", url, body)
		ch <- urls
	}
}

// task allocator
func master(ch chan []string, fetcher Fetcher) {
	// how many live tasks
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch {
		for _, u := range urls {
			if _, status := fetched[u]; status == false {
				fetched[u] = true
				n += 1
				go doFetch(u, fetcher, ch)
			}
		}
		// one task has been finished
		n -= 1
		if n == 0 {
			break
		}
	}
}

func CrawlChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() { ch <- []string{url} }()
	master(ch, fetcher)
}

func main() {
	fmt.Printf("Crawl with serialization\n")
	CrawlSerialized("http://golang.org/", 4, fetcher)
	fmt.Printf("--------------------\n")
	fmt.Printf("Crawl parallel with mutex&waitGroup\n")
	CrawlMutex("http://golang.org/", 4, fetcher, makeFetchState())
	fmt.Printf("--------------------\n")
	fmt.Printf("Crawl parallel with channels\n")
	CrawlChannel("http://golang.org/", fetcher)
	fmt.Printf("--------------------\n")
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"http://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}
