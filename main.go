package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/r4d1n/marsrover"
)

var mars *marsrover.Client
var pool = newPool()

// just the information to make a next request for photos by sol
type reqTuple struct {
	rover string
	sol   int
}

func init() {
	mars = marsrover.NewClient(os.Getenv("NASA_API_KEY"))
}

func main() {
	fillCache()
}

func fillCache() {
	var wg sync.WaitGroup
	rovers := []string{"curiosity", "opportunity", "spirit"}
	mchan := make(chan []string)          // channel of rovers to get manifests
	schan := make(chan reqTuple)          // channel of sols to make rate limited requests
	limiter := time.Tick(time.Second * 4) // rate limiting ticker channel to stay under 1000 reqs / hr
	done := make(map[string]bool)
	go func() { mchan <- rovers }()
	for list := range mchan {
		for _, r := range list {
			if !done[r] {
				done[r] = true
				fmt.Println(r)
				wg.Add(1)
				go func(n string) {
					defer wg.Done()
					// get and cache most recent manifest per rover
					manifest, err := updateManifest(n)
					if err != nil {
						handleStatusError(err)
					}
					// add sols to channel to be fetched
					for _, s := range manifest.Sols {
						schan <- reqTuple{n, s.Sol}
					}
					// range over channel and fetch sols with rate limit
					for req := range schan {
						<-limiter
						err := cacheSol(req.rover, req.sol)
						if err != nil {
							handleStatusError(err)
						}
					}
					fmt.Printf("Completed rover: %s \n", n)
					return
				}(r)
			}
			wg.Wait()
			os.Exit(0)
		}
	}
}

func updateManifest(r string) (*marsrover.Manifest, error) {
	var err error
	conn := pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("manifest:%s", r)
	var data *marsrover.Manifest
	var j []byte
	data, err = mars.GetManifest(r)
	if err != nil {
		return nil, err
	}
	j, err = json.Marshal(&data)
	_, err = conn.Do("SET", key, j)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func cacheSol(r string, s int) error {
	var err error
	conn := pool.Get()
	defer conn.Close()
	var data *marsrover.PhotoResponse
	var j []byte
	key := fmt.Sprintf("sol:%s:%d", r, s)
	if reply, _ := conn.Do("GET", key); reply != nil && reply != "null" {
		fmt.Printf("%s is in the cache \n", key)
		j = reply.([]byte)
		err = json.Unmarshal(j, data)
	} else {
		fmt.Printf("%s is NOT in the cache \n", key)
		data, err = mars.GetImagesBySol(r, s)
		j, err = json.Marshal(data)
		_, err = conn.Do("SET", key, j)
		if err != nil {
			return err
		}
	}
	return nil
}

func handleStatusError(err error) {
	switch e := err.(type) {
	case *marsrover.StatusError:
		if e.Status() == 429 {
			// wait an hour for rate limit to expire to try again
			log.Printf("Status %d. Exceeded Rate Limit. Waiting To Try Again... \n", e.Status())
			time.Sleep(time.Hour)
			fillCache()
		} else {
			log.Fatal(e)
		}
	default:
		log.Fatal(e)
	}
}
