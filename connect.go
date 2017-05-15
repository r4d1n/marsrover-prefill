package main

import "github.com/garyburd/redigo/redis"

const rport = ":6379"

func newConn() (redis.Conn, error) {
	conn, err := redis.Dial("tcp", rport)
	return conn, err
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}
