package main

import (
	"container/heap"
	"encoding/json"
	"sync"
	"time"
	"estreito-de-ormuz/shared"
)

type itemFila struct {
	msg        shared.Mensagem
	prioridade int
	index      int
	inseridoEm time.Time
}

type heapFila []*itemFila

func (h heapFila) Len() int { return len(h) }

// Maior prioridade primeiro
func (h heapFila) Less(i, j int) bool { return h[i].prioridade > h[j].prioridade }
func (h heapFila) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *heapFila) Push(x any) { *h = append(*h, x.(*itemFila)) }
func (h *heapFila) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

type FilaPrioridade struct {
	mu   sync.Mutex
	heap heapFila
	ch   chan struct{}
}

func NewFilaPrioridade() *FilaPrioridade {
	f := &FilaPrioridade{ch: make(chan struct{}, 100)}
	heap.Init(&f.heap)
	return f
}

func (f *FilaPrioridade) Adicionar(msg shared.Mensagem) {
	var evento shared.EventoMaritimo
	json.Unmarshal(msg.Payload, &evento)

	f.mu.Lock()
	heap.Push(&f.heap, &itemFila{
		msg:        msg,
		prioridade: evento.Prioridade,
		inseridoEm: time.Now(),
	})
	f.mu.Unlock()

	f.ch <- struct{}{}
}

// Bloqueia até ter um item disponível
func (f *FilaPrioridade) Proximo() shared.Mensagem {
	<-f.ch
	f.mu.Lock()
	defer f.mu.Unlock()
	item := heap.Pop(&f.heap).(*itemFila)
	return item.msg
}

// Aging: incrementa prioridade de itens que esperam há mais de 10s
func (f *FilaPrioridade) IniciarAging() {
	go func() {
		for {
			time.Sleep(10 * time.Second)
			f.mu.Lock()
			alterou := false
			for _, item := range f.heap {
				if item.prioridade < 5 && time.Since(item.inseridoEm) > 10*time.Second {
					item.prioridade++
					alterou = true
				}
			}
			if alterou {
				heap.Init(&f.heap) // reordena após mudança
			}
			f.mu.Unlock()
		}
	}()
}