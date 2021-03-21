package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "log"

func init() {
	if _, err := os.Stat("mrcoordinator.log"); err == nil {
		os.Remove("mrcoordinator.log")
	 }

	f, err := os.OpenFile("mrcoordinator.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return
	}

	log.SetOutput(f)
	log.Println("Log file for coordinator.")
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalln(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
