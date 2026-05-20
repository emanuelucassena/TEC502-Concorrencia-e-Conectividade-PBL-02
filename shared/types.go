package shared

import "time"

type TipoEvento string

const (
	BloqueioRota          TipoEvento = "BLOQUEIO_ROTA"
	EmbarcacaoDeriva      TipoEvento = "EMBARCACAO_DERIVA"
	ObjetoNaoIdentificado TipoEvento = "OBJETO_NAO_IDENTIFICADO"
	Congestionamento      TipoEvento = "CONGESTIONAMENTO"
	InspecaoUrgente       TipoEvento = "INSPECAO_URGENTE"
)

type StatusDrone string

const (
	Disponivel StatusDrone = "DISPONIVEL"
	Reservado  StatusDrone = "RESERVADO"
	EmMissao   StatusDrone = "EM_MISSAO"
	Falha      StatusDrone = "FALHA"
)

type TipoMensagem string

const (
	MsgEvento          TipoMensagem = "EVENTO"
	MsgRequest         TipoMensagem = "REQUEST"
	MsgOK              TipoMensagem = "OK"
	MsgDespacho        TipoMensagem = "DESPACHO"
	MsgHeartbeat       TipoMensagem = "HEARTBEAT"
	MsgMissaoConcluida TipoMensagem = "MISSAO_CONCLUIDA"
	MsgRegistroDrone   TipoMensagem = "REGISTRO_DRONE"
    MsgEncaminhar    TipoMensagem = "ENCAMINHAR"
    MsgSemDrone      TipoMensagem = "SEM_DRONE"
)

// Mensagem base — carrega o clock de Lamport em todas as comunicações
type Mensagem struct {
	Tipo    TipoMensagem `json:"tipo"`
	Payload []byte       `json:"payload"`
	De      string       `json:"de"`
	Clock   int          `json:"clock"`
}

type EventoMaritimo struct {
	ID         string     `json:"id"`
	Tipo       TipoEvento `json:"tipo"`
	Prioridade int        `json:"prioridade"`
	SetorID    string     `json:"setor_id"`
	Timestamp  time.Time  `json:"timestamp"`
	Clock      int        `json:"clock"`
}

type RequestLock struct {
	BrokerID   string    `json:"broker_id"`
	DroneID    string    `json:"drone_id"`
	Clock      int       `json:"clock"`
	Prioridade int       `json:"prioridade"`
	Timestamp  time.Time `json:"timestamp"`
}

type Heartbeat struct {
	DroneID   string      `json:"drone_id"`
	Status    StatusDrone `json:"status"`
	Timestamp time.Time   `json:"timestamp"`
	Clock     int         `json:"clock"`
}

type MissaoConcluida struct {
	DroneID   string    `json:"drone_id"`
	Timestamp time.Time `json:"timestamp"`
	Clock     int       `json:"clock"`
}

// Registro inicial do drone ao conectar
type RegistroDrone struct {
	DroneID string `json:"drone_id"`
	Addr    string `json:"addr"`
	Clock   int    `json:"clock"`
}