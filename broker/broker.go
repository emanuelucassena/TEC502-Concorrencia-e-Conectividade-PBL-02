package main

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"estreito-de-ormuz/shared"
)

type Broker struct {
	id     string
	porta  string
	peers  []string
	fila   *FilaPrioridade
	ricart *RicartManager
}

func NewBroker(id, porta string, peers []string) *Broker {
	b := &Broker{
		id:    id,
		porta: porta,
		peers: peers,
		fila:  NewFilaPrioridade(),
	}
	b.fila.IniciarAging()
	b.ricart = NewRicartManager(id, peers, b.fila)
	return b
}

func (b *Broker) Iniciar() {
	go b.escutar()
	go b.processarFila()
	go b.monitorarDrones()

	select {}
}

func (b *Broker) escutar() {
	ln, err := net.Listen("tcp", ":"+b.porta)
	if err != nil {
		log.Fatalf("[Broker-%s] falha ao escutar: %v", b.id, err)
	}
	log.Printf("[Broker-%s] escutando em :%s", b.id, b.porta)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go b.handleConexao(conn)
	}
}

func (b *Broker) handleConexao(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := shared.ReceberMensagem(conn)
		if err != nil {
			return
		}
		switch msg.Tipo {
		case shared.MsgEvento:
			b.receberEvento(msg)
		case shared.MsgOK:
			b.ricart.ReceberOK(msg)
		case shared.MsgRequest:
			b.ricart.ReceberRequest(msg, conn)
		case shared.MsgRegistroDrone:
			var reg shared.RegistroDrone
			json.Unmarshal(msg.Payload, &reg)
			b.ricart.RegistrarDrone(reg.DroneID, reg.Addr)
		case shared.MsgHeartbeat:
			b.atualizarHeartbeat(msg)
		case shared.MsgMissaoConcluida:
			b.droneConcluiu(msg)
		case shared.MsgEncaminhar:
			b.ricart.ReceberEncaminhamento(msg, conn)
		}
	}
}

func (b *Broker) receberEvento(msg shared.Mensagem) {
	log.Printf("[Broker-%s] evento recebido de %s clock=%d", b.id, msg.De, msg.Clock)
	b.fila.Adicionar(msg)
}

func (b *Broker) processarFila() {
	for {
		msg := b.fila.Proximo()
		log.Printf("[Broker-%s] processando evento da fila clock=%d", b.id, msg.Clock)
		b.ricart.SolicitarLock(msg)

		// MODO APRESENTAÇÃO: Força o broker a congelar por 4 segundos
		// antes de puxar o próximo evento da fila.
		
		time.Sleep(4 * time.Second)
	}
}

func (b *Broker) monitorarDrones() {
	go b.ricart.MonitorarHeartbeats()
}

func (b *Broker) atualizarHeartbeat(msg shared.Mensagem) {
	var hb shared.Heartbeat
	json.Unmarshal(msg.Payload, &hb)
	b.ricart.AtualizarHeartbeat(hb.DroneID, hb.Clock)
}

func (b *Broker) droneConcluiu(msg shared.Mensagem) {
	var mc shared.MissaoConcluida
	json.Unmarshal(msg.Payload, &mc)
	b.ricart.DroneConcluiu(mc.DroneID, mc.Clock)
}
