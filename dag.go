package main

var dags []Dag

func getDags() []Dag {
	return []Dag{NewSimpleDAG()}
}
