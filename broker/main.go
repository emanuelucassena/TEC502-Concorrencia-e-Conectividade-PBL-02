package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	id := os.Getenv("BROKER_ID")
	porta := os.Getenv("PORTA")
	peersStr := os.Getenv("PEERS")

	var peers []string
	if strings.TrimSpace(peersStr) != "" {
		peers = strings.Split(peersStr, ",")
	} else {
		log.Printf("[Broker-%s] Aviso: Iniciando sem peers configurados.", id)
	}

	log.Printf("[Broker-%s] iniciando na porta %s", id, porta)

	b := NewBroker(id, porta, peers)
	go b.Iniciar()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	log.Printf("[Broker-%s] sinal de encerramento recebido. Desligando com segurança...", id)
}