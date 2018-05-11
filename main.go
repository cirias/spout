package main

import (
	"fmt"
	"log"
	"sync"
)

func init() {
	dags = getDags()
}

func main() {
	// TODO init from database
	lookup := &SyncMapLookup{&sync.Map{}}

	listeners := make(map[string][]chan Item)

	jobs := make(chan interface{})

	go func() {
		for job := range jobs {
			log.Println("-->", job)
		}
	}()

	for _, dag := range dags {
		for _, node := range dag.Nodes() {
			items := make(chan Item)

			for _, r := range node.Inputs() {
				resourceId := getResouceId(r)

				if chs, ok := listeners[resourceId]; ok {
					chs = append(chs, items)
				} else {
					listeners[resourceId] = []chan Item{items}
				}
			}

			go Run(lookup.Get, node, items, jobs)
		}
	}

	items, err := GetItems()
	if err != nil {
		log.Fatalln("could not get items:", err)
	}

	log.Println("reading items")
	for item := range items {
		lookup.Set(item)

		resourceId := fmt.Sprintf("%s/%s", item.Dag(), item.Resource())
		chs, ok := listeners[resourceId]
		if !ok {
			// TODO log and ignore
			continue
		}

		for _, ch := range chs {
			ch <- item
		}
	}
}
