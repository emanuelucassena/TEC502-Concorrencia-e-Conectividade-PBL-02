package main

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"estreito-de-ormuz/shared"
)

type estadoRequisicao int

const (
	livre estadoRequisicao = iota
	querendo
	detendo
)

type pendente struct {
	conn net.Conn
	msg  shared.Mensagem
}

type infoDrone struct {
	id       string
	addr     string
	status   shared.StatusDrone
	ultimoHB time.Time
}

type RicartManager struct {
	mu               sync.Mutex
	brokerID         string
	peers            []string
	clock            int
	estado           estadoRequisicao
	filaEspera       []pendente
	canalOK          chan struct{}
	dronesConhecidos map[string]*infoDrone
	filaBroker       *FilaPrioridade
}

func NewRicartManager(brokerID string, peers []string, fila *FilaPrioridade) *RicartManager {
	return &RicartManager{
		brokerID:         brokerID,
		peers:            peers,
		estado:           livre,
		canalOK:          make(chan struct{}, 100),
		dronesConhecidos: make(map[string]*infoDrone),
		filaBroker:       fila,
	}
}

func (r *RicartManager) atualizarClock(clockRecebido int) {
	if clockRecebido > r.clock {
		r.clock = clockRecebido
	}
	r.clock++
}

func (r *RicartManager) RegistrarDrone(id, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, existe := r.dronesConhecidos[id]; !existe {
		r.dronesConhecidos[id] = &infoDrone{
			id:       id,
			addr:     addr,
			status:   shared.Disponivel,
			ultimoHB: time.Now(),
		}
		log.Printf("[Ricart-%s] drone registrado: %s addr=%s", r.brokerID, id, addr)
	}
}

func (r *RicartManager) AtualizarHeartbeat(droneID string, clockRecebido int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.atualizarClock(clockRecebido)
	if d, ok := r.dronesConhecidos[droneID]; ok {
		d.ultimoHB = time.Now()
		// RECUPERAÇÃO: Tira o drone do estado Zumbi se a rede voltar
		if d.status == shared.Falha {
			log.Printf("[Ricart-%s] drone %s recuperado de falha", r.brokerID, droneID)
			d.status = shared.Disponivel
		}
	}
}

func (r *RicartManager) escolherDrone() *infoDrone {
	for _, d := range r.dronesConhecidos {
		if d.status == shared.Disponivel {
			return d
		}
	}
	return nil
}

func (r *RicartManager) SolicitarLock(msg shared.Mensagem) {
	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	r.estado = querendo
	clockAtual := r.clock
	r.mu.Unlock()

	if len(r.peers) == 0 {
		r.entrarSecaoCritica(msg)
		return
	}

	req := shared.RequestLock{
		BrokerID:   r.brokerID,
		Clock:      clockAtual,
		Prioridade: extrairPrioridade(msg),
		Timestamp:  time.Now(),
	}
	payload, _ := json.Marshal(req)
	requisicao := shared.Mensagem{
		Tipo:    shared.MsgRequest,
		Payload: payload,
		De:      r.brokerID,
		Clock:   clockAtual,
	}

	for _, peer := range r.peers {
		go r.enviarParaPeer(peer, requisicao)
	}

	timeout := time.After(5 * time.Second)
	recebidos := 0
	for recebidos < len(r.peers) {
		select {
		case <-r.canalOK:
			recebidos++
		case <-timeout:
			log.Printf("[Ricart-%s] timeout aguardando OKs", r.brokerID)
			recebidos = len(r.peers)
		}
	}

	r.entrarSecaoCritica(msg)
}

func (r *RicartManager) entrarSecaoCritica(msg shared.Mensagem) {
	r.mu.Lock()
	r.estado = detendo
	drone := r.escolherDrone()
	if drone != nil {
		drone.status = shared.Reservado
	}
	r.mu.Unlock()

	if drone == nil {
		log.Printf("[Ricart-%s] sem drone disponível, encaminhando para peers", r.brokerID)
		encaminhado := r.encaminharParaPeers(msg)
		if !encaminhado {
			log.Printf("[Ricart-%s] nenhum peer disponível, aguardando liberação de drones...", r.brokerID)

			
			go func() {
				time.Sleep(2 * time.Second) 
				r.filaBroker.Adicionar(msg)
			}()
		}
		r.liberarLock()
		return
	}

	log.Printf("[Ricart-%s] despachando drone %s clock=%d", r.brokerID, drone.id, r.clock)
	r.despacharDrone(drone, msg)
	r.liberarLock()
}

func (r *RicartManager) encaminharParaPeers(msg shared.Mensagem) bool {
	r.mu.Lock()
	clock := r.clock + 1
	r.clock = clock
	r.mu.Unlock()

	encaminhamento := shared.Mensagem{
		Tipo:    shared.MsgEncaminhar,
		Payload: msg.Payload,
		De:      r.brokerID,
		Clock:   clock,
	}

	for _, peer := range r.peers {
		if r.tentarEncaminhar(peer, encaminhamento) {
			log.Printf("[Ricart-%s] requisição encaminhada para %s", r.brokerID, peer)
			return true
		}
	}
	return false
}

func (r *RicartManager) tentarEncaminhar(peer string, msg shared.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", peer, 3*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	if err := shared.EnviarMensagem(conn, msg); err != nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resposta, err := shared.ReceberMensagem(conn)
	if err != nil {
		return false
	}

	return resposta.Tipo != shared.MsgSemDrone
}

func (r *RicartManager) despacharDrone(drone *infoDrone, msg shared.Mensagem) {
	conn, err := net.DialTimeout("tcp", drone.addr, 3*time.Second)
	if err != nil {
		log.Printf("[Ricart-%s] falha ao conectar drone %s: %v", r.brokerID, drone.id, err)
		r.mu.Lock()
		drone.status = shared.Falha
		r.mu.Unlock()
		r.filaBroker.Adicionar(msg)
		return
	}
	defer conn.Close()

	r.mu.Lock()
	r.clock++
	clock := r.clock
	r.mu.Unlock()

	despacho := shared.Mensagem{
		Tipo:    shared.MsgDespacho,
		Payload: msg.Payload,
		De:      r.brokerID,
		Clock:   clock,
	}

	// LATÊNCIA ARTIFICIAL: Pausa antes de mandar a missão para o Drone
	time.Sleep(2 * time.Second)
	shared.EnviarMensagem(conn, despacho)

	r.mu.Lock()
	drone.status = shared.EmMissao
	r.mu.Unlock()
}

func (r *RicartManager) liberarLock() {
	r.mu.Lock()
	r.estado = livre
	espera := make([]pendente, len(r.filaEspera))
	copy(espera, r.filaEspera)
	r.filaEspera = nil
	r.clock++
	clock := r.clock
	r.mu.Unlock()

	ok := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: clock,
	}
	for _, p := range espera {
		time.Sleep(1 * time.Second)
		shared.EnviarMensagem(p.conn, ok)
		log.Printf("[Ricart-%s] liberando OK retido enviando para outro Broker", r.brokerID)
	}
}

func (r *RicartManager) ReceberRequest(msg shared.Mensagem, conn net.Conn) {
	var req shared.RequestLock
	json.Unmarshal(msg.Payload, &req)

	r.mu.Lock()
	r.atualizarClock(msg.Clock)

	ok := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: r.clock,
	}

	deveEsperar := r.estado == detendo ||
		(r.estado == querendo && (r.clock < req.Clock ||
			(r.clock == req.Clock && r.brokerID < req.BrokerID)))

	if deveEsperar {
		r.filaEspera = append(r.filaEspera, pendente{conn: conn, msg: msg})
		r.mu.Unlock()
		log.Printf("[Ricart-%s] segurando OK para %s clock=%d", r.brokerID, req.BrokerID, req.Clock)
	} else {
		r.mu.Unlock()
		time.Sleep(1 * time.Second)

		shared.EnviarMensagem(conn, ok)
		log.Printf("[Ricart-%s] OK enviado para %s clock=%d", r.brokerID, req.BrokerID, req.Clock)
	}
}

func (r *RicartManager) ReceberOK(msg shared.Mensagem) {
	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	r.mu.Unlock()
	r.canalOK <- struct{}{}
}

func (r *RicartManager) ReceberEncaminhamento(msg shared.Mensagem, conn net.Conn) {
	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	drone := r.escolherDrone()
	if drone != nil {
		drone.status = shared.Reservado
	}
	r.clock++
	clock := r.clock
	r.mu.Unlock()

	if drone == nil {
		log.Printf("[Ricart-%s] encaminhamento recusado — sem drone", r.brokerID)
		recusa := shared.Mensagem{
			Tipo:  shared.MsgSemDrone,
			De:    r.brokerID,
			Clock: clock,
		}
		shared.EnviarMensagem(conn, recusa)
		return
	}

	log.Printf("[Ricart-%s] encaminhamento aceito — despachando %s", r.brokerID, drone.id)
	aceite := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: clock,
	}
	shared.EnviarMensagem(conn, aceite)
	r.despacharDrone(drone, msg)
}

func (r *RicartManager) DroneConcluiu(droneID string, clockRecebido int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.atualizarClock(clockRecebido)
	if d, ok := r.dronesConhecidos[droneID]; ok {
		d.status = shared.Disponivel
		log.Printf("[Ricart-%s] drone %s disponível clock=%d", r.brokerID, droneID, r.clock)
	}
}

func (r *RicartManager) MonitorarHeartbeats() {
	for {
		time.Sleep(5 * time.Second)
		r.mu.Lock()
		for _, d := range r.dronesConhecidos {
			if d.status != shared.Falha && time.Since(d.ultimoHB) > 9*time.Second {
				log.Printf("[Ricart-%s] drone %s FALHA por timeout", r.brokerID, d.id)
				d.status = shared.Falha
			}
		}
		r.mu.Unlock()
	}
}

func (r *RicartManager) enviarParaPeer(addr string, msg shared.Mensagem) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		log.Printf("[Ricart-%s] peer %s indisponível", r.brokerID, addr)
		r.canalOK <- struct{}{}
		return
	}

	shared.EnviarMensagem(conn, msg)

	go func() {
		defer conn.Close()
		resposta, err := shared.ReceberMensagem(conn)
		if err == nil && resposta.Tipo == shared.MsgOK {
			r.ReceberOK(resposta)
		} else {
			r.canalOK <- struct{}{}
		}
	}()
}

func extrairPrioridade(msg shared.Mensagem) int {
	var evento shared.EventoMaritimo
	json.Unmarshal(msg.Payload, &evento)
	return evento.Prioridade
}
