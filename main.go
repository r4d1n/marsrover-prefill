package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
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
		go func(v string) { mchan <- v }(rov)
	}
	limiter := time.Tick(time.Millisecond * 1000) // rate limiting ticker channel
	for _, r := range rovers {
		if !done[r] {
			done[r] = true
			wg.Add(1)
			go func(n string) {
				fmt.Printf("Beginning rover: %s \n", n)
				defer wg.Done()
				// get and cache most recent manifest per rover
				manifest, err := cacheManifest(n)
				sols := manifest.Sols
				if err != nil {
					handleStatusError(err)
				}
				errchan := make(chan error)
				conn := pool.Get()
				defer conn.Close()
				if reply, _ := conn.Do("GET", fmt.Sprintf("rover:%s:sol:last", n)); reply != nil {
					s, err := redis.String(reply, nil)
					k, err := strconv.Atoi(s)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("rover:%s:sol:last is %d -- will resume...\n", n, k)
					for i, val := range manifest.Sols {
						if val.Sol == k {
							sols = sols[i+1:]
							break
						}
					}
				}
				for _, s := range sols {
					// fetch sol data with rate limit
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
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("SET", key, j)
	_, err = conn.Do("EXPIRE", key, int(time.Hour*24))
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
	} else {
		fmt.Printf("%s is NOT in the cache \n", key)
		data, err = mars.GetImagesBySol(r, s)
		j, err = json.Marshal(data)
		if err != nil {
			handleStatusError(err)
		} else {
			_, err = conn.Do("SET", key, j)
			// set last cached sol for this rover to start from same place next time
			_, err = conn.Do("SET", fmt.Sprintf("rover:%s:sol:last", r), s)
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
			log.Printf("error %d: exceeded rate limit. waiting to try again...\n", e.Status())
			time.Sleep(time.Minute * 30)
			fillCache()
		} else {
			log.Fatal(e)
		}
	default:
		log.Fatal(e)
	}
}
