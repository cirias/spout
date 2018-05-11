package main

import "testing"

func TestSimple(t *testing.T) {
	ct0 := &ChunkType{Start: 0, Duration: 20}
	ct1 := &ChunkType{Start: 0, Duration: 30}

	r0 := &ChunkResource{Name_: "r0", Dag_: "simple", ChunkType: ct0}
	r1 := &ChunkResource{Name_: "r1", Dag_: "simple", ChunkType: ct1}

	err := SendItem(&Chunk{
		Type:          r0.Name(),
		ChunkResource: r0,
		RunId:         "run_0",
		Index:         0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = SendItem(&Chunk{
		Type:          r1.Name(),
		ChunkResource: r1,
		RunId:         "run_0",
		Index:         0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = SendItem(&Chunk{
		Type:          r0.Name(),
		ChunkResource: r0,
		RunId:         "run_0",
		Index:         1,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = SendItem(&Chunk{
		Type:          r1.Name(),
		ChunkResource: r1,
		RunId:         "run_0",
		Index:         1,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = SendItem(&Chunk{
		Type:          r0.Name(),
		ChunkResource: r0,
		RunId:         "run_0",
		Index:         2,
	})
	if err != nil {
		t.Fatal(err)
	}
}
