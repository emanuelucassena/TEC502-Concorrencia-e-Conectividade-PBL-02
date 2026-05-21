package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	id := os.Getenv("DRONE_ID")
	porta := os.Getenv("PORTA")
	addr := os.Getenv("ADDR") 
	brokersStr := os.Getenv("BROKERS")

	if id == "" || porta == "" || addr == "" {
		log.Fatalf("[ERRO FATAL] DRONE_ID, PORTA e ADDR são obrigatórios. Defina os IPs corretamente.")
	}

	var brokers []string
	if strings.TrimSpace(brokersStr) != "" {
		brokers = strings.Split(brokersStr, ",")
	}

	log.Printf("[Drone-%s] iniciando na porta %s addr=%s", id, porta, addr)

	d := NewDrone(id, porta, addr)
	go d.Iniciar(brokers)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	log.Printf("[Drone-%s] Sinal recebido. Encerrando o drone...", id)
}