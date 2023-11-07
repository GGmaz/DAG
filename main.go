package main

import (
	"fmt"
	"sync"
	"time"
)

type Status string

const (
	Pending Status = "Pending"
	Passed         = "Passed"
	Failed         = "Failed"
)

type Vertex struct {
	ID      string
	Status  Status
	Loop    int
	Dag     *Dag
	CanFail bool
}

func (v *Vertex) Id() string {
	return v.ID
}

func (v *Vertex) SetPass() {
	v.Dag.mu.Lock()
	defer v.Dag.mu.Unlock()
	if !(v.Dag.HasFailed()) {
		v.Status = Passed
	}
}

func (v *Vertex) SetFail() {
	v.Dag.mu.Lock()
	defer v.Dag.mu.Unlock()
	if !(v.Dag.HasFailed()) {
		v.Status = Failed
		if !v.CanFail {
			println("Vertex: " + v.Id() + " failed, but it cannot fail.")
			v.Dag.Status = Failed
		}
	}
}

func (v *Vertex) State() {
	fmt.Printf("Vertex %s is in state: %s\n", v.ID, v.Status)
}

type Dag struct {
	Vertices            map[string]*Vertex
	ConnectionsParents  map[string][]string
	ConnectionsChildren map[string][]string
	mu                  sync.Mutex
	Status              Status
	IsStarted           bool
}

func (d *Dag) Next() []Vertex {
	d.IsStarted = true

	if d.HasFailed() {
		panic("DAG has failed")
	}

	var nextVertices []Vertex
	for _, v := range d.Vertices {
		if d.CanExecute(v) {
			nextVertices = append(nextVertices, *v)
		}
	}
	return nextVertices
}

func (d *Dag) CanExecute(v *Vertex) bool {
	for _, parentID := range d.ConnectionsParents[v.ID] {
		parent := d.Vertices[parentID]
		if parent == nil || parent.Status == Pending {
			return false
		}
	}
	if v.Status == Pending {
		return true
	}
	return false
}

func (d *Dag) HasFailed() bool {
	return d.Status == Failed
}

func (d *Dag) HasSucceeded() bool {
	return d.Status == Passed
}

func (d *Dag) HasFinished() bool {
	return d.HasFailed() || d.HasSucceeded()
}

func (d *Dag) AddVertex(v *Vertex) {
	if d.IsStarted {
		panic("Cannot add vertex to a DAG that has already started")
	}
	v.Dag = d
	d.Vertices[v.ID] = v
}

func (d *Dag) AddEdge(from, to string) {
	if d.IsStarted {
		panic("Cannot add edge to a DAG that has already started")
	}

	if d.IsCyclic(from, to) {
		panic("Cannot add cyclic edge " + from + " -> " + to)
	}

	d.ConnectionsParents[to] = append(d.ConnectionsParents[to], from)
	d.ConnectionsChildren[from] = append(d.ConnectionsChildren[from], to)
}

func (d *Dag) IsCyclic(from, to string) bool {
	for _, child := range d.ConnectionsChildren[to] {
		if child == from {
			return true
		}
		if d.IsCyclic(from, child) {
			return true
		}
	}
	return false
}

func (d *Dag) ExecuteVertices() {
	for !d.HasFinished() {
		verticesToProcess := d.Next()

		if verticesToProcess == nil {
			d.Status = Passed
		}

		var wg sync.WaitGroup
		for _, v := range verticesToProcess {
			wg.Add(v.Loop)
			for i := 0; i < v.Loop; i++ {
				go func(vertex *Vertex) {
					defer wg.Done()
					//executing vertex
					if time.Now().Nanosecond()%2 == 0 {
						d.Vertices[vertex.Id()].SetPass()
					} else {
						d.Vertices[vertex.Id()].SetFail()
					}
				}(&v)
			}
		}

		wg.Wait()
	}
}

func NewDag() *Dag {
	return &Dag{
		Vertices:            make(map[string]*Vertex),
		ConnectionsParents:  make(map[string][]string),
		ConnectionsChildren: make(map[string][]string),
		Status:              Pending,
		IsStarted:           false,
	}
}

func main() {
	dag := NewDag()

	vertexA := &Vertex{ID: "A", Status: Pending, CanFail: true, Loop: 1}
	vertexB := &Vertex{ID: "B", Status: Pending, CanFail: false, Loop: 1}
	vertexC := &Vertex{ID: "C", Status: Pending, CanFail: true, Loop: 11}
	vertexD := &Vertex{ID: "D", Status: Pending, CanFail: true, Loop: 3}
	vertexE := &Vertex{ID: "E", Status: Pending, CanFail: true, Loop: 1}

	dag.AddVertex(vertexA)
	dag.AddVertex(vertexB)
	dag.AddVertex(vertexC)
	dag.AddVertex(vertexD)
	dag.AddVertex(vertexE)

	dag.AddEdge("A", "B")
	//dag.AddEdge("B", "A")
	dag.AddEdge("A", "C")
	dag.AddEdge("B", "D")
	dag.AddEdge("C", "D")
	dag.AddEdge("D", "E")
	dag.AddEdge("A", "D")
	//dag.AddEdge("D", "A")
	dag.AddEdge("A", "E")
	//dag.AddEdge("E", "A")
	dag.AddEdge("C", "E")

	dag.ExecuteVertices()

	if dag.HasFailed() {
		fmt.Println("DAG has failed.")
	} else if dag.HasSucceeded() {
		fmt.Println("DAG has succeeded.")
	}
}
