package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"estreito-de-ormuz/shared"
)

var tiposEvento = []shared.TipoEvento{
	shared.BloqueioRota,
	shared.EmbarcacaoDeriva,
	shared.ObjetoNaoIdentificado,
	shared.Congestionamento,
	shared.InspecaoUrgente,
}

type Sensor struct {
	mu         sync.Mutex
	setorID    string
	brokerAddr string
	intervalo  int
	clock      int
	contador   int
}

func NewSensor(setorID, brokerAddr string, intervalo int) *Sensor {
	return &Sensor{
		setorID:    setorID,
		brokerAddr: brokerAddr,
		intervalo:  intervalo,
	}
}

// Incrementa e retorna o clock — thread-safe
func (s *Sensor) tickClock() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock++
	return s.clock
}

func (s *Sensor) Iniciar() {
	for {
		conn, err := net.Dial("tcp", s.brokerAddr)
		if err != nil {
			log.Printf("[Sensor-%s] broker indisponível, tentando em 3s...", s.setorID)
			time.Sleep(3 * time.Second)
			continue
		}
		s.loop(conn)
		conn.Close()
	}
}

func (s *Sensor) loop(conn net.Conn) {
	for {
		clock := s.tickClock()
		evento := s.gerarEvento(clock)
		payload, _ := json.Marshal(evento)

		msg := shared.Mensagem{
			Tipo:    shared.MsgEvento,
			Payload: payload,
			De:      "sensor-" + s.setorID,
			Clock:   clock,
		}

		if err := shared.EnviarMensagem(conn, msg); err != nil {
			log.Printf("[Sensor-%s] erro ao enviar: %v", s.setorID, err)
			return
		}

		log.Printf("[Sensor-%s] evento enviado clock=%d tipo=%s prioridade=%d",
			s.setorID, clock, evento.Tipo, evento.Prioridade)

		espera := time.Duration(s.intervalo+rand.Intn(s.intervalo)) * time.Millisecond
		time.Sleep(espera)
	}
}

func (s *Sensor) gerarEvento(clock int) shared.EventoMaritimo {
	s.mu.Lock()
	s.contador++
	id := fmt.Sprintf("sensor-%s-%d-%d", s.setorID, time.Now().UnixNano(), s.contador)
	s.mu.Unlock()

	return shared.EventoMaritimo{
		ID:         id,
		Tipo:       tiposEvento[rand.Intn(len(tiposEvento))],
		Prioridade: rand.Intn(5) + 1,
		SetorID:    s.setorID,
		Timestamp:  time.Now(),
		Clock:      clock,
	}
}