# Estreito de Ormuz

> **Disciplina:** TEC502 — MI Concorrência e Conectividade 
> **Universidade Estadual de Feira de Santana (UEFS)**  
> **Problema 2 — Desbloqueio do Estreito de Ormuz**

---

## O que é este projeto?

Este sistema simula uma infraestrutura distribuída para coordenar uma frota de **drones autônomos de monitoramento marítimo** no Estreito de Ormuz.

Sensores espalhados por diferentes setores detectam eventos marítimos (embarcações à deriva, bloqueios de rota, objetos não identificados etc.) e os reportam a brokers regionais. Esses brokers disputam — de forma segura e coordenada — o despacho de drones para atender as ocorrências.

Todo o sistema opera sem nenhum servidor central. Os brokers se comunicam diretamente entre si via **sockets TCP**, usando o algoritmo de **Ricart-Agrawala** para garantir que o mesmo drone nunca seja enviado para dois lugares ao mesmo tempo.

---

## Arquitetura

```
[Sensor A] ──► [Broker A] ◄─── TCP ───► [Broker B] ◄── [Sensor B]
                   │                          │
                   └────── TCP ──────► [Broker C] ◄── [Sensor C]
                                           │
                                    [Frota de Drones]
                             drone-1 | drone-2 | drone-3 | drone-4
```

### Componentes

| Componente | Responsabilidade |
|---|---|
| **Broker** | Gerencia um setor marítimo. Recebe eventos dos sensores, solicita exclusão mútua com os peers e despacha drones. |
| **Sensor** | Gera eventos marítimos aleatórios em intervalos configuráveis e os envia ao broker do seu setor. |
| **Drone** | Executa missões, envia heartbeat a cada 3s e notifica o broker quando a missão é concluída. |
| **Shared** | Pacote Go compartilhado com todos os tipos de mensagem e o protocolo TCP. |

---

## Algoritmos e Mecanismos

### Ricart-Agrawala — Exclusão Mútua Distribuída

Garante que um drone nunca seja despachado para duas missões ao mesmo tempo, mesmo quando múltiplos brokers fazem requisições simultâneas.

**Funcionamento:**
1. Um broker que quer despachar um drone incrementa seu clock e envia `REQUEST` para todos os peers.
2. Cada peer responde com `OK` imediatamente — a menos que ele próprio esteja disputando o mesmo recurso com clock menor, caso em que segura o `OK`.
3. O broker que recebe `OK` de todos entra na seção crítica e despacha o drone.
4. Ao sair, envia `OK` para todos que estavam esperando.

### Relógio Lógico de Lamport

Presente em **todos** os componentes. Garante uma ordem total de eventos no sistema distribuído.

- **Ao enviar:** `clock++`
- **Ao receber:** `clock = max(meuClock, clockRecebido) + 1`

### Fila de Prioridade com Aging

Cada broker mantém uma fila local onde eventos com maior prioridade são atendidos primeiro. Para evitar **inanição** (um evento nunca ser atendido), o mecanismo de *aging* incrementa a prioridade de qualquer evento que esteja esperando há mais de 10 segundos.

### Heartbeat e Detecção de Falha

Cada drone envia um `HEARTBEAT` a cada **3 segundos** para todos os brokers. Se nenhum heartbeat chegar em **9 segundos**, o drone é marcado como `FALHA` e o evento retorna à fila automaticamente.

### Encaminhamento entre Setores

Se um broker não tiver drone disponível, ele tenta encaminhar a requisição para seus peers antes de recolocar na própria fila. O primeiro peer com drone livre aceita e faz o despacho. Se nenhum peer tiver drone, a requisição volta para a fila local com delay de 2 segundos.

---

## Protocolo de Comunicação

Comunicação via **sockets TCP puros** com mensagens em **JSON**. Para evitar o problema de framing do TCP (mensagens chegando coladas), cada mensagem é prefixada com um **header de 4 bytes** indicando o tamanho do payload.

### Tipos de Mensagem

| Tipo | Direção | Descrição |
|---|---|---|
| `EVENTO` | Sensor → Broker | Ocorrência detectada pelo sensor |
| `REQUEST` | Broker → Broker | Solicitação de lock (Ricart-Agrawala) |
| `OK` | Broker → Broker | Confirmação do lock |
| `DESPACHO` | Broker → Drone | Ordem de missão |
| `HEARTBEAT` | Drone → Broker | Sinal de vida |
| `MISSAO_CONCLUIDA` | Drone → Broker | Notificação de missão finalizada |
| `REGISTRO_DRONE` | Drone → Broker | Registro inicial com endereço real do drone |
| `ENCAMINHAR` | Broker → Broker | Repassa requisição para peer com drone disponível |
| `SEM_DRONE` | Broker → Broker | Recusa de encaminhamento por falta de drone |

---

## Estrutura do Repositório

```
estreito-de-ormuz/
├── broker/
│   ├── main.go           — ponto de entrada do broker
│   ├── broker.go         — roteamento de mensagens e lógica principal
│   ├── ricart.go         — Ricart-Agrawala e gerência de drones
│   ├── fila.go           — fila de prioridade com aging
│   └── Dockerfile.broker
│
├── sensor/
│   ├── main.go           — ponto de entrada do sensor
│   ├── sensor.go         — gerador de eventos aleatórios
│   └── Dockerfile.sensor
│
├── drone/
│   ├── main.go           — ponto de entrada do drone
│   ├── drone.go          — execução de missões, heartbeat e registro
│   └── Dockerfile.drone
│
├── shared/
│   ├── types.go          — structs e constantes compartilhadas
│   └── protocolo.go      — encode/decode de mensagens TCP
│
├── docker-compose.yml          — modo de teste local (1 máquina)
├── docker-compose.pc1.yml      — modo de apresentação: PC 1
├── docker-compose.pc2.yml      — modo de apresentação: PC 2
├── docker-compose.pc3.yml      — modo de apresentação: PC 3
├── docker-compose.pc4.yml      — modo de apresentação: PC 4
└── go.mod
```

---

## Arquivos Docker Compose — Por que existem dois modos?

O projeto tem **dois conjuntos de arquivos Docker Compose** para finalidades diferentes:

### `docker-compose.yml` — Teste local em uma única máquina

Este arquivo foi criado para **desenvolvimento e testes** durante a construção do projeto, quando não havia acesso a quatro máquinas em rede. Todos os serviços — brokers, drones e sensores — sobem juntos no mesmo host, usando os **nomes dos containers como endereços** (ex: `broker-a:5001`), que são resolvidos pela rede interna do Docker.

Use este arquivo para rodar e testar o sistema inteiro de forma rápida, sem precisar de infraestrutura especial.

```bash
docker compose up --build
```

### `docker-compose.pc1.yml` a `docker-compose.pc4.yml` — Apresentação em 4 máquinas

Estes quatro arquivos foram criados especificamente para a **apresentação no laboratório**, onde cada máquina física executa uma parte do sistema. Como as máquinas se comunicam pela rede local — e não pela rede interna do Docker — os endereços são **IPs fixos** da rede do laboratório.

| Arquivo | Máquina | IP | Serviços |
|---|---|---|---|
| `docker-compose.pc1.yml` | PC 1 | `172.16.103.2` | broker-a, sensor-a, drone-1 |
| `docker-compose.pc2.yml` | PC 2 | `172.16.103.3` | broker-b, sensor-b, drone-2 |
| `docker-compose.pc3.yml` | PC 3 | `172.16.103.4` | broker-c, sensor-c, drone-3 |
| `docker-compose.pc4.yml` | PC 4 | `172.16.103.5` | broker-d, broker-e, sensor-d, sensor-e, drone-4 |

> **Resumo:** `docker-compose.yml` é para rodar tudo localmente. Os arquivos `pc*.yml` são para distribuir os serviços em máquinas físicas separadas.

---

## Como Executar

### Pré-requisitos

- [Go 1.22+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/)

---

### Opção 1 — Teste local (1 máquina)

A forma mais simples de rodar o sistema inteiro. Basta clonar e subir:

```bash
git clone https://github.com/seu-usuario/estreito-de-ormuz.git
cd estreito-de-ormuz

docker compose up --build
```

Para parar todos os serviços:

```bash
docker compose down
```

---

### Opção 2 — Apresentação distribuída (4 máquinas em rede local)

**Passo 1 — Libere as portas no firewall de cada máquina:**

```bash
sudo ufw allow from 172.16.103.0/24
```

**Passo 2 — Suba o PC 4 primeiro.**

Os drones precisam estar ativos para que os brokers possam registrá-los ao iniciar.

```bash
# No PC 4 (172.16.103.5)
docker compose -f docker-compose.pc4.yml up --build
```

**Passo 3 — Suba os PCs 1, 2 e 3 em qualquer ordem:**

```bash
# No PC 1 (172.16.103.2)
docker compose -f docker-compose.pc1.yml up --build

# No PC 2 (172.16.103.3)
docker compose -f docker-compose.pc2.yml up --build

# No PC 3 (172.16.103.4)
docker compose -f docker-compose.pc3.yml up --build
```

---

### Adicionando um drone em tempo de execução

É possível adicionar um novo drone **sem reiniciar nada**. Ele se registra automaticamente em todos os brokers e fica disponível para despacho imediatamente:

```bash
docker run -d \
  --network host \
  -e DRONE_ID=drone-5 \
  -e PORTA=6005 \
  -e ADDR=172.16.103.5 \
  -e BROKERS=172.16.103.2:5001,172.16.103.3:5002,172.16.103.4:5003,172.16.103.5:5004,172.16.103.5:5005 \
  estreito-de-ormuz-drone
```

No `docker-compose.pc1.yml`, o `drone-5` já está definido com o perfil `extra` para facilitar este cenário:

```bash
docker compose -f docker-compose.pc1.yml --profile extra up drone-5
```

---

## Variáveis de Ambiente

### Broker

| Variável | Descrição | Exemplo |
|---|---|---|
| `BROKER_ID` | Identificador único do broker | `A` |
| `PORTA` | Porta TCP de escuta | `5001` |
| `PEERS` | Endereços dos outros brokers (vírgula) | `172.16.103.3:5002,172.16.103.4:5003` |

### Sensor

| Variável | Descrição | Exemplo |
|---|---|---|
| `SETOR_ID` | Identificador do setor monitorado | `A` |
| `BROKER_ADDR` | Endereço do broker destino | `172.16.103.2:5001` |
| `INTERVALO_MS` | Intervalo base entre eventos em ms | `12000` |

### Drone

| Variável | Descrição | Exemplo |
|---|---|---|
| `DRONE_ID` | Identificador único do drone | `drone-1` |
| `PORTA` | Porta TCP de escuta para despachos | `6001` |
| `ADDR` | IP da máquina onde o drone está rodando | `172.16.103.5` |
| `BROKERS` | Endereços de todos os brokers (vírgula) | `172.16.103.2:5001,...` |

---

## Tolerância a Falhas

| Cenário | O que acontece |
|---|---|
| Drone para de responder | Heartbeat timeout em 9s → drone marcado como `FALHA` → evento recolocado na fila |
| Novo drone entra em runtime | Envia `REGISTRO_DRONE` → registrado automaticamente → disponível de imediato |
| Broker sem drone disponível | Tenta encaminhar para peers → se nenhum tiver drone, recoloca na fila local com delay de 2s |
| Peer broker indisponível | Timeout de 5s no Ricart-Agrawala → sistema continua operando normalmente |
| Evento esperando há muito tempo | Aging incrementa prioridade a cada 10s → evento eventualmente atendido |
| Drone se recupera após falha | Próximo heartbeat recebido → drone volta ao estado `DISPONIVEL` automaticamente |

---

## Tecnologias Utilizadas

- **Go 1.22** — linguagem principal de todo o sistema
- **TCP Sockets** — transporte de mensagens entre todos os componentes (sem middleware externo)
- **JSON** — serialização das mensagens
- **Docker / Docker Compose** — containerização e orquestração
- **Algoritmo de Ricart-Agrawala** — exclusão mútua distribuída entre brokers
- **Relógio Lógico de Lamport** — ordenação causal de eventos no sistema distribuído
