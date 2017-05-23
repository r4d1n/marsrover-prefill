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

func init() {
	mars = marsrover.NewClient(os.Getenv("NASA_API_KEY"))
}

func main() {
	fillCache()
}

func fillCache() {
	var wg sync.WaitGroup
	rovers := []string{"spirit", "opportunity", "curiosity"}
	mchan := make(chan string) // channel of rovers to get manifests
	done := make(map[string]bool)
	for _, rov := range rovers {
		go func() { mchan <- rov }()
	}
	limiter := time.Tick(time.Millisecond * 1000) // rate limiting ticker channel
	for _, r := range rovers {
		fmt.Printf("Beginning rover: %s \n", r)
		if !done[r] {
			done[r] = true
			wg.Add(1)
			go func(n string) {
				defer wg.Done()
				// get and cache most recent manifest per rover
				manifest, err := cacheManifest(n)
				if err != nil {
					handleStatusError(err)
				}
				errchan := make(chan error)
				// add sols to channel to be fetched
				// for _, s := range manifest.Sols {
				for i := len(manifest.Sols) - 1; i >= 0; i-- {
					s := manifest.Sols[i]
					// range over channel and fetch sols with rate limit
					<-limiter
					go func() {
						errchan <- cacheSol(n, s.Sol)
					}()
					err := <-errchan
					if err != nil {
						handleStatusError(err)
					}
				}
				fmt.Printf("Completed rover: %s \n", n)
				return
			}(r)
		}
	}
	wg.Wait()
	os.Exit(0)
}

func cacheManifest(r string) (*marsrover.Manifest, error) {
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
	key := fmt.Sprintf("rover:%s:sol:%d", r, s)
	if reply, _ := conn.Do("GET", key); reply != nil && string(reply.([]byte)) != "null" {
		fmt.Printf("%s is in the cache \n", key)
		j = reply.([]byte)
		err = json.Unmarshal(j, data)
	} else {
		fmt.Printf("%s is NOT in the cache \n", key)
		data, err = mars.GetImagesBySol(r, s)
		j, err = json.Marshal(data)
		if string(j) == "null" {
			e := marsrover.StatusError{
				Code: 429,
				Msg:  "Exceeded rate limit",
			}
			handleStatusError(e)
		} else {
			_, err = conn.Do("SET", key, j)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func handleStatusError(err error) {
	switch e := err.(type) {
	case *marsrover.StatusError:
		if e.Status() == 429 {
			// wait for rate limit to expire to try again
			log.Printf("Error %d: Exceeded Rate Limit. Waiting To Try Again... \n", e.Status())
			time.Sleep(time.Minute * 30)
			fillCache()
		} else {
			log.Fatal(e)
		}
	default:
		log.Fatal(e)
	}
}
