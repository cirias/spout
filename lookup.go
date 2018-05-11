package main

import (
	"fmt"
	"sync"
)

type Lookup interface {
	Get(Item) interface{}
	Set(Item, interface{})
}

type SyncMapLookup struct {
	m *sync.Map
}

func getItemKey(item Item) string {
	return fmt.Sprintf("%s/%s/%s/%s", item.Dag(), item.Run(), item.Resource(), item.Key())
}

func (m *SyncMapLookup) Get(item Item) Item {
	key := getItemKey(item)

	r, ok := m.m.Load(key)
	if ok {
		return r.(Item)
	}

	return nil
}

func (m *SyncMapLookup) Set(item Item) {
	key := getItemKey(item)

	m.m.Store(key, item)
}
