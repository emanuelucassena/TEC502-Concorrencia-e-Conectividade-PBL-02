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
	addr   string 
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
	log.Printf("🚀 [Sistema-%s] Iniciando motores e calibrando sensores...", d.id)
	go d.escutar()
	go d.heartbeatLoop(brokers)
	go d.executarMissoes(brokers)

	select {}
}

func (d *Drone) escutar() {
	ln, err := net.Listen("tcp", ":"+d.porta)
	if err != nil {
		log.Fatalf("[ERRO-%s] 💥 Falha crítica no rádio: %v", d.id, err)
	}
	log.Printf("📡 [Rádio-%s] Frequência aberta. Aguardando coordenadas na porta %s", d.id, d.porta)

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
			log.Printf("📥 [Comando-%s] Recebida ordem de despacho da malha de Brokers! (Clock: %d)", d.id, msg.Clock)
			select {
			case d.missao <- msg:
			default:
				log.Printf("⚠️ [Comando-%s] Ordem ignorada: O drone já encontra-se em operação fora da base.", d.id)
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

	// Cálculo dinâmico de tempo: Ocorrências mais difíceis (Prioridade 1/2) demoram mais.
	
	tempoBase := 3
	pesoPrioridade := evento.Prioridade 
	duracao := time.Duration(tempoBase+pesoPrioridade) * time.Second

	log.Printf("\n🚁 ═════════ DECOLAGEM: %s ═════════", d.id)
	log.Printf("📍 Destino    : Setor %s", evento.SetorID)
	log.Printf("🔥 Ocorrência : %s", evento.Tipo)
	log.Printf("⏱️ ETA        : Tempo estimado de voo e resolução de %d segundos.", int(duracao.Seconds()))
	log.Printf("═══════════════════════════════════════════════\n")

	// Simula o tempo de viagem e resolução do problema no mar
	time.Sleep(duracao)

	d.mu.Lock()
	d.status = shared.Disponivel
	d.mu.Unlock()

	log.Printf("✅ [Retorno-%s] Operação '%s' no Setor %s bem-sucedida! Notificando a malha de Brokers...", d.id, evento.Tipo, evento.SetorID)
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
	
	time.Sleep(3 * time.Second)

	
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

	log.Printf("🛰️ [Telemetria-%s] Transmitindo registro de IP (%s) para pareamento com a malha...", d.id, d.addr)

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