package main

import (
	"fmt"
)

type Dag interface {
	Name() string
	Nodes() []Node
	Resources() []Resource
}

type Node interface {
	Inputs() []Resource
	Resolve(ItemFinder, Item) []interface{}
}

type Resource interface {
	Name() string
	Dag() string
	NewItem() Item
}

/*
 * type Item interface {
 *   Dag      string `json:"dag"`
 *   RunId    string `json:"runId"`
 *   Resource string `json:"resource"`
 *   Key      string `json:"key"`
 * }
 */

type Item interface {
	Dag() string
	Resource() string
	Run() string
	Key() string
}

type ItemFinder func(Item) Item

func getResouceId(r Resource) string {
	return fmt.Sprintf("%s/%s", r.Dag(), r.Name())
}

func Run(findItem ItemFinder, node Node, in <-chan Item, out chan<- interface{}) {
	for item := range in {
		jobs := node.Resolve(findItem, item)

		for _, job := range jobs {
			out <- job
		}
	}
}
