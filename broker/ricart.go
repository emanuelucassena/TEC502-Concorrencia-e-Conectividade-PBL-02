package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
	"math/rand"

	"estreito-de-ormuz/shared"
)

var debugLamport = os.Getenv("DEBUG_LAMPORT") == "true"

type estadoRequisicao int

const (
	livre estadoRequisicao = iota
	querendo
	detendo
)

func (e estadoRequisicao) String() string {
	switch e {
	case livre:
		return "LIVRE"
	case querendo:
		return "QUERENDO"
	case detendo:
		return "DETENDO(SC)"
	default:
		return "DESCONHECIDO"
	}
}

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

func linha(brokerID, titulo string) {
	log.Printf("\n[Broker-%s] ══════════ %s ══════════", brokerID, titulo)
}

func (r *RicartManager) atualizarClock(clockRecebido int) {
	anterior := r.clock
	if clockRecebido > r.clock {
		r.clock = clockRecebido
	}
	r.clock++
	if debugLamport {
		log.Printf("[Broker-%s] ⏱️ [LAMPORT] %d + recebido=%d => novo=%d",
			r.brokerID, anterior, clockRecebido, r.clock)
	}
}

func (r *RicartManager) contarDronesDisponiveis() int {
	count := 0
	for _, d := range r.dronesConhecidos {
		if d.status == shared.Disponivel {
			count++
		}
	}
	return count
}

func (r *RicartManager) resumoDrones() string {
	if len(r.dronesConhecidos) == 0 {
		return "Nenhum drone registrado"
	}
	resultado := ""
	for _, d := range r.dronesConhecidos {
		resultado += fmt.Sprintf("[%s:%s] ", d.id, d.status)
	}
	return resultado
}


//  REGISTRO E HEARTBEAT


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
		log.Printf("[Broker-%s] 📡 DRONE CONECTADO | %s (%s) | Frota: %s", r.brokerID, id, addr, r.resumoDrones())
	}
}

func (r *RicartManager) AtualizarHeartbeat(droneID string, clockRecebido int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.atualizarClock(clockRecebido)
	if d, ok := r.dronesConhecidos[droneID]; ok {
		d.ultimoHB = time.Now()
		if d.status == shared.Falha {
			d.status = shared.Disponivel
			log.Printf("[Broker-%s] ♻️ SINAL RESTABELECIDO | Drone %s online novamente | Frota: %s", r.brokerID, droneID, r.resumoDrones())
		}
	}
}

func (r *RicartManager) escolherDrone() *infoDrone {
	var disponiveis []*infoDrone
	
	// Filtra todos os que estão realmente livres no momento
	for _, d := range r.dronesConhecidos {
		if d.status == shared.Disponivel {
			disponiveis = append(disponiveis, d)
		}
	}
	
	// Se não tiver nenhum, retorna nil para acionar a terceirização
	if len(disponiveis) == 0 {
		return nil
	}
	
	// Sorteia um drone aleatório entre os disponíveis

	indiceSorteado := rand.Intn(len(disponiveis))
	return disponiveis[indiceSorteado]
}


//  RICART-AGRAWALA — SOLICITAR LOCK


func (r *RicartManager) SolicitarLock(msg shared.Mensagem) {
	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	r.estado = querendo
	clockAtual := r.clock
	disponiveis := r.contarDronesDisponiveis()
	total := len(r.dronesConhecidos)
	r.mu.Unlock()

	evento := extrairEvento(msg)

	linha(r.brokerID, "ALERTA MARÍTIMO DETECTADO")
	log.Printf("[Broker-%s] 🚨 Ocorrência : %s", r.brokerID, evento.Tipo)
	log.Printf("[Broker-%s] 📍 Local      : Setor %s", r.brokerID, evento.SetorID)
	log.Printf("[Broker-%s] ⚡ Prioridade : Nível %d", r.brokerID, evento.Prioridade)
	log.Printf("[Broker-%s] 🚁 Frota      : %d/%d livres | %s", r.brokerID, disponiveis, total, r.resumoDrones())

	time.Sleep(500 * time.Millisecond)

	if len(r.peers) == 0 {
		log.Printf("[Broker-%s] 🟡 Operando em modo isolado. Assumindo controle direto.", r.brokerID)
		r.entrarSecaoCritica(msg)
		return
	}

	req := shared.RequestLock{
		BrokerID:   r.brokerID,
		Clock:      clockAtual,
		Prioridade: evento.Prioridade,
		Timestamp:  time.Now(),
	}
	payload, _ := json.Marshal(req)
	requisicao := shared.Mensagem{
		Tipo:    shared.MsgRequest,
		Payload: payload,
		De:      r.brokerID,
		Clock:   clockAtual,
	}

	log.Printf("[Broker-%s] 📡 Iniciando protocolo Ricart-Agrawala com %d pares...", r.brokerID, len(r.peers))
	for _, peer := range r.peers {
		go r.enviarParaPeer(peer, requisicao)
	}

	timeout := time.After(8 * time.Second)
	recebidos := 0
	for recebidos < len(r.peers) {
		select {
		case <-r.canalOK:
			recebidos++
			log.Printf("[Broker-%s] ✅ Autorização concedida (%d/%d)", r.brokerID, recebidos, len(r.peers))
		case <-timeout:
			log.Printf("[Broker-%s] ⚠️ TIMEOUT. Assumindo lock via Fail-Fast (%d/%d)", r.brokerID, recebidos, len(r.peers))
			recebidos = len(r.peers)
		}
	}

	r.entrarSecaoCritica(msg)
}


//  RICART-AGRAWALA — SEÇÃO CRÍTICA


func (r *RicartManager) entrarSecaoCritica(msg shared.Mensagem) {
	r.mu.Lock()
	r.estado = detendo
	drone := r.escolherDrone()
	if drone != nil {
		drone.status = shared.Reservado
	}
	r.mu.Unlock()

	evento := extrairEvento(msg)

	linha(r.brokerID, "LOCK ADQUIRIDO - SEÇÃO CRÍTICA")

	if drone == nil {
		log.Printf("[Broker-%s] ❌ Frota ocupada. Tentando repassar missão '%s' para outro Broker...", r.brokerID, evento.Tipo)
		encaminhado := r.encaminharParaPeers(msg)
		if !encaminhado {
			log.Printf("[Broker-%s] ⏸️ Rede saturada. Evento de prioridade %d adicionado à fila local.", r.brokerID, evento.Prioridade)
			go func() {
				time.Sleep(3 * time.Second)
				r.filaBroker.Adicionar(msg)
			}()
		}
		r.liberarLock()
		return
	}

	log.Printf("[Broker-%s] 🎯 ALVO TRAVADO: Drone '%s' selecionado para a missão.", r.brokerID, drone.id)
	time.Sleep(1 * time.Second) // Pausa dramática para a apresentação

	sucesso := r.despacharDrone(drone, msg)

	r.mu.Lock()
	if sucesso {
		drone.status = shared.EmMissao
		log.Printf("[Broker-%s] 🚀 DECOLAGEM AUTORIZADA! Drone '%s' a caminho do Setor %s para lidar com %s.", r.brokerID, drone.id, evento.SetorID, evento.Tipo)
	} else {
		drone.status = shared.Disponivel
		log.Printf("[Broker-%s] 💥 ABORTAR! Drone '%s' não respondeu ao comando de decolagem.", r.brokerID, drone.id)
	}
	r.mu.Unlock()

	r.liberarLock()
}


//  ENCAMINHAMENTO ENTRE BROKERS


func (r *RicartManager) encaminharParaPeers(msg shared.Mensagem) bool {
	r.mu.Lock()
	r.clock++
	clock := r.clock
	r.mu.Unlock()

	encaminhamento := shared.Mensagem{
		Tipo:    shared.MsgEncaminhar,
		Payload: msg.Payload,
		De:      r.brokerID,
		Clock:   clock,
	}

	for _, peer := range r.peers {
		log.Printf("[Broker-%s] 🔀 Negociando transferência de missão com %s...", r.brokerID, peer)
		time.Sleep(500 * time.Millisecond)
		if r.tentarEncaminhar(peer, encaminhamento) {
			log.Printf("[Broker-%s] 🤝 Sucesso! Broker %s assumiu a responsabilidade.", r.brokerID, peer)
			return true
		}
	}
	log.Printf("[Broker-%s] ❌ Nenhum Broker na malha possui drones livres.", r.brokerID)
	return false
}

func (r *RicartManager) tentarEncaminhar(peer string, msg shared.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", peer, 8*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	if err := shared.EnviarMensagem(conn, msg); err != nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	resposta, err := shared.ReceberMensagem(conn)
	if err != nil {
		return false
	}

	return resposta.Tipo != shared.MsgSemDrone
}

//  DESPACHO AO DRONE
func (r *RicartManager) despacharDrone(drone *infoDrone, msg shared.Mensagem) bool {
	conn, err := net.DialTimeout("tcp", drone.addr, 8*time.Second)
	if err != nil {
		r.mu.Lock()
		drone.status = shared.Falha
		r.mu.Unlock()
		r.filaBroker.Adicionar(msg)
		return false
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

	if err := shared.EnviarMensagem(conn, despacho); err != nil {
		return false
	}

	return true
}

//  LIBERAR LOCK


func (r *RicartManager) liberarLock() {
	r.mu.Lock()
	r.estado = livre
	espera := make([]pendente, len(r.filaEspera))
	copy(espera, r.filaEspera)
	r.filaEspera = nil
	r.clock++
	clock := r.clock
	r.mu.Unlock()

	log.Printf("[Broker-%s] 🔓 Lock liberado. Processando %d Brokers na fila de espera.", r.brokerID, len(espera))

	ok := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: clock,
	}

	for _, p := range espera {
		shared.EnviarMensagem(p.conn, ok)
	}
}


//  RECEBER REQUEST / OK

func (r *RicartManager) ReceberRequest(msg shared.Mensagem, conn net.Conn) {
	var req shared.RequestLock
	json.Unmarshal(msg.Payload, &req)

	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	estadoAtual := r.estado
	clockAtual := r.clock

	ok := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: clockAtual,
	}

	deveEsperar := estadoAtual == detendo ||
		(estadoAtual == querendo && (clockAtual < req.Clock ||
			(clockAtual == req.Clock && r.brokerID < req.BrokerID)))

	if deveEsperar {
		r.filaEspera = append(r.filaEspera, pendente{conn: conn, msg: msg})
		r.mu.Unlock()
		log.Printf("[Broker-%s] 🛑 CONFLITO DETECTADO! Minha prioridade é maior. Segurando autorização para Broker-%s.", r.brokerID, req.BrokerID)
	} else {
		r.mu.Unlock()
		shared.EnviarMensagem(conn, ok)
		log.Printf("[Broker-%s] 👍 Aprovando acesso para Broker-%s", r.brokerID, req.BrokerID)
	}
}

func (r *RicartManager) ReceberOK(msg shared.Mensagem) {
	r.mu.Lock()
	r.atualizarClock(msg.Clock)
	r.mu.Unlock()
	r.canalOK <- struct{}{}
}


//  RECEBER ENCAMINHAMENTO


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

	evento := extrairEvento(msg)

	if drone == nil {
		recusa := shared.Mensagem{
			Tipo:  shared.MsgSemDrone,
			De:    r.brokerID,
			Clock: clock,
		}
		shared.EnviarMensagem(conn, recusa)
		return
	}

	log.Printf("[Broker-%s] 🤝 Missão de suporte aceita! Redirecionando Drone '%s' para resolver '%s' a pedido do Broker-%s.", r.brokerID, drone.id, evento.Tipo, msg.De)

	aceite := shared.Mensagem{
		Tipo:  shared.MsgOK,
		De:    r.brokerID,
		Clock: clock,
	}
	shared.EnviarMensagem(conn, aceite)

	sucesso := r.despacharDrone(drone, msg)

	r.mu.Lock()
	if sucesso {
		drone.status = shared.EmMissao
	} else {
		drone.status = shared.Disponivel
	}
	r.mu.Unlock()
}


//  MISSÃO CONCLUÍDA E MONITORAMENTO
func (r *RicartManager) DroneConcluiu(droneID string, clockRecebido int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.atualizarClock(clockRecebido)
	if d, ok := r.dronesConhecidos[droneID]; ok {
		d.status = shared.Disponivel
		log.Printf("\n[Broker-%s] 🏁 OPERAÇÃO FINALIZADA | Drone '%s' retornou à base com sucesso e está livre para novos despachos.", r.brokerID, droneID)
	}
}

func (r *RicartManager) MonitorarHeartbeats() {
	for {
		time.Sleep(5 * time.Second)
		r.mu.Lock()
		for _, d := range r.dronesConhecidos {
			if d.status != shared.Falha && time.Since(d.ultimoHB) > 9*time.Second {
				log.Printf("[Broker-%s] 🚨 PERDA DE SINAL: Drone '%s' não reporta status há mais de 9s. Marcado como INOPERANTE.", r.brokerID, d.id)
				d.status = shared.Falha
			}
		}
		r.mu.Unlock()
	}
}


//  COMUNICAÇÃO COM PEERS

func (r *RicartManager) enviarParaPeer(addr string, msg shared.Mensagem) {
	conn, err := net.DialTimeout("tcp", addr, 8*time.Second)
	if err != nil {
		r.canalOK <- struct{}{}
		return
	}

	if err := shared.EnviarMensagem(conn, msg); err != nil {
		conn.Close()
		r.canalOK <- struct{}{}
		return
	}

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


//  UTILITÁRIOS


func extrairEvento(msg shared.Mensagem) shared.EventoMaritimo {
	var evento shared.EventoMaritimo
	json.Unmarshal(msg.Payload, &evento)
	return evento
}

func extrairPrioridade(msg shared.Mensagem) int {
	return extrairEvento(msg).Prioridade
}