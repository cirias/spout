package main

import (
	"fmt"
)

// Dag
type SimpleDAG struct {
	name      string
	nodes     []Node
	resources []Resource
}

func (d *SimpleDAG) Name() string {
	return "simple"
}

func (d *SimpleDAG) Nodes() []Node {
	return d.nodes
}

func (d *SimpleDAG) Resources() []Resource {
	return d.resources
}

func NewSimpleDAG() Dag {
	d := &SimpleDAG{}

	ct0 := &ChunkType{Start: 0, Duration: 20}
	ct1 := &ChunkType{Start: 0, Duration: 30}

	r0 := &ChunkResource{Name_: "r0", Dag_: d.Name(), ChunkType: ct0}
	r1 := &ChunkResource{Name_: "r1", Dag_: d.Name(), ChunkType: ct1}

	n0 := &SimpleNode{manifest: r0, inputs: []*ChunkResource{r0, r1}}

	d.nodes = []Node{n0}
	d.resources = []Resource{r0, r1}

	return d
}

// Node
type SimpleNode struct {
	manifest *ChunkResource
	inputs   []*ChunkResource
}

func (n *SimpleNode) Inputs() []Resource {
	rs := make([]Resource, len(n.inputs))
	for i, r := range n.inputs {
		rs[i] = r
	}
	return rs
}

func (n *SimpleNode) Resolve(finder ItemFinder, item Item) []interface{} {
	jobs := []interface{}{}

	for _, mr := range n.manifest.Intersect(item) {
		job := n.GetJob(finder, mr, item)
		if job != nil {
			jobs = append(jobs, job)
			break
		}
	}

	return jobs
}

func (n *SimpleNode) GetJob(finder ItemFinder, manifestItem Item, item Item) []Item {
	job := []Item{}

	for _, input := range n.inputs {
		inputItemIndexes := input.Intersect(manifestItem)

		for _, index := range inputItemIndexes {
			item := finder(index)
			if item == nil {
				return nil
			}

			job = append(job, item)
		}
	}

	return job
}

// Item
type ChunkType struct {
	Start    int64 `json:"start"`
	Duration int64 `json:"duration"`
}

func (lhs *ChunkType) Equal(rhs *ChunkType) bool {
	return lhs.Start == rhs.Start && lhs.Duration == rhs.Duration
}

func (ct *ChunkType) Intersect(ch *Chunk) (int64, int64) {
	srcStart := ch.ChunkResource.ChunkType.Start
	srcDuration := ch.ChunkResource.ChunkType.Duration

	dstStart := ct.Start
	dstDuration := ct.Duration

	dstOffset := srcStart + ch.Index*srcDuration - dstStart

	startIndex := dstOffset / dstDuration
	endIndex := (dstOffset + srcDuration) / dstDuration
	if (dstOffset+srcDuration)%dstDuration != 0 {
		endIndex = endIndex + 1
	}

	return startIndex, endIndex - startIndex
}

type ChunkResource struct {
	Name_     string     `json:"name"`
	Dag_      string     `json:"dag"`
	ChunkType *ChunkType `json:"chunkType"`
}

func (r *ChunkResource) NewItem() Item {
	return &Chunk{}
}

func (r *ChunkResource) Name() string {
	return r.Name_
}

func (r *ChunkResource) Dag() string {
	return r.Dag_
}

func (r *ChunkResource) Intersect(item Item) []Item {
	switch item.(type) {
	case *Chunk:
		ck := item.(*Chunk)

		if ck.ChunkResource.ChunkType.Equal(r.ChunkType) {
			return []Item{&Chunk{
				ChunkResource: r,
				RunId:         ck.RunId,
				Index:         ck.Index,
			}}
		}

		start, count := r.ChunkType.Intersect(ck)

		rs := make([]Item, 0, count)
		for i := start; i < start+count; i += 1 {
			rs = append(rs, &Chunk{
				ChunkResource: r,
				RunId:         ck.RunId,
				Index:         i,
			})
		}

		return rs

	default:
		// TODO return error?
		return []Item{}
	}
}

type Chunk struct {
	Type          string         `json:"type"`
	ChunkResource *ChunkResource `json:"chunkResource"`
	RunId         string         `json:"runId"`
	Index         int64          `json:"index"`
}

func (c *Chunk) Dag() string {
	return c.ChunkResource.Dag_
}

func (c *Chunk) Run() string {
	return c.RunId
}

func (c *Chunk) Resource() string {
	return c.ChunkResource.Name()
}

func (c *Chunk) Key() string {
	return fmt.Sprintf("%05d", c.Index)
}
