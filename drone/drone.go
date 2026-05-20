package main

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"estreito-de-ormuz/shared"
)

type Drone struct {
	mu     sync.Mutex
	id     string
	porta  string
	addr   string // IP público + porta — enviado no registro
	status shared.StatusDrone
	clock  int
	missao chan shared.Mensagem
}

func NewDrone(id, porta, addr string) *Drone {
	return &Drone{
		id:     id,
		porta:  porta,
		addr:   addr + ":" + porta,
		status: shared.Disponivel,
		missao: make(chan shared.Mensagem, 1),
	}
}

func (d *Drone) atualizarClock(clockRecebido int) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if clockRecebido > d.clock {
		d.clock = clockRecebido
	}
	d.clock++
	return d.clock
}

func (d *Drone) tickClock() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.clock++
	return d.clock
}

func (d *Drone) Iniciar(brokers []string) {
	go d.escutar()
	go d.heartbeatLoop(brokers)
	go d.executarMissoes(brokers)

	select {}
}

func (d *Drone) escutar() {
	ln, err := net.Listen("tcp", ":"+d.porta)
	if err != nil {
		log.Fatalf("[Drone-%s] falha ao escutar: %v", d.id, err)
	}
	log.Printf("[Drone-%s] aguardando despachos em :%s", d.id, d.porta)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.handleConexao(conn)
	}
}

func (d *Drone) handleConexao(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := shared.ReceberMensagem(conn)
		if err != nil {
			return
		}
		d.atualizarClock(msg.Clock)

		if msg.Tipo == shared.MsgDespacho {
			log.Printf("[Drone-%s] despacho recebido clock=%d", d.id, msg.Clock)
			select {
			case d.missao <- msg:
			default:
				log.Printf("[Drone-%s] já em missão, despacho ignorado", d.id)
			}
		}
	}
}

func (d *Drone) executarMissoes(brokers []string) {
	for msg := range d.missao {
		d.executarMissao(msg, brokers)
	}
}

func (d *Drone) executarMissao(msg shared.Mensagem, brokers []string) {
	d.mu.Lock()
	d.status = shared.EmMissao
	d.mu.Unlock()

	var evento shared.EventoMaritimo
	json.Unmarshal(msg.Payload, &evento)

	log.Printf("[Drone-%s] missão iniciada: tipo=%s setor=%s prioridade=%d",
		d.id, evento.Tipo, evento.SetorID, evento.Prioridade)

	duracao := time.Duration(10+evento.Prioridade*2) * time.Second
	time.Sleep(duracao)

	d.mu.Lock()
	d.status = shared.Disponivel
	d.mu.Unlock()

	log.Printf("[Drone-%s] missão concluída", d.id)
	d.notificarMissaoConcluida(brokers)
}

func (d *Drone) notificarMissaoConcluida(brokers []string) {
	clock := d.tickClock()

	concluida := shared.MissaoConcluida{
		DroneID:   d.id,
		Timestamp: time.Now(),
		Clock:     clock,
	}
	payload, _ := json.Marshal(concluida)

	msg := shared.Mensagem{
		Tipo:    shared.MsgMissaoConcluida,
		Payload: payload,
		De:      d.id,
		Clock:   clock,
	}

	for _, addr := range brokers {
		go func(a string) {
			conn, err := net.DialTimeout("tcp", a, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			shared.EnviarMensagem(conn, msg)
		}(addr)
	}
}

func (d *Drone) heartbeatLoop(brokers []string) {
	// Aguarda brokers subirem
	time.Sleep(2 * time.Second)

	// Registro inicial — informa endereço real ao broker
	d.enviarRegistro(brokers)

	for {
		d.mu.Lock()
		status := d.status
		d.mu.Unlock()

		clock := d.tickClock()

		hb := shared.Heartbeat{
			DroneID:   d.id,
			Status:    status,
			Timestamp: time.Now(),
			Clock:     clock,
		}
		payload, _ := json.Marshal(hb)

		msg := shared.Mensagem{
			Tipo:    shared.MsgHeartbeat,
			Payload: payload,
			De:      d.id,
			Clock:   clock,
		}

		for _, addr := range brokers {
			go d.enviarParaBroker(addr, msg)
		}

		time.Sleep(3 * time.Second)
	}
}

func (d *Drone) enviarRegistro(brokers []string) {
	clock := d.tickClock()

	reg := shared.RegistroDrone{
		DroneID: d.id,
		Addr:    d.addr, // endereço real de escuta
		Clock:   clock,
	}
	payload, _ := json.Marshal(reg)

	msg := shared.Mensagem{
		Tipo:    shared.MsgRegistroDrone,
		Payload: payload,
		De:      d.id,
		Clock:   clock,
	}

	log.Printf("[Drone-%s] enviando registro addr=%s", d.id, d.addr)

	for _, addr := range brokers {
		go d.enviarParaBroker(addr, msg)
	}
}

func (d *Drone) enviarParaBroker(addr string, msg shared.Mensagem) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	shared.EnviarMensagem(conn, msg)
}