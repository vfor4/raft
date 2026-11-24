package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	for true {
		c := make(chan bool)

		for i := range 3 {
			go func(idx int) {
				c <- grantVote(idx)
			}(i)
		}

		votes := 0
		for range 3 {
			if <-c {
				votes++
			}

			if votes >= 2 {
				fmt.Println("FIND LEADER")
			}
		}
	}

}

func grantVote(i int) bool {
	log.Printf("Call[%v]", i)
	time.Sleep(1 * time.Second)
	return i != 0
}
