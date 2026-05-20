package main

import (
	"log"
	"os"
	"strings"
)

func main() {
	id := os.Getenv("DRONE_ID")
	porta := os.Getenv("PORTA")
	addr := os.Getenv("ADDR")         // IP público do PC onde o drone roda
	brokersStr := os.Getenv("BROKERS")
	brokers := strings.Split(brokersStr, ",")

	log.Printf("[Drone-%s] iniciando na porta %s addr=%s", id, porta, addr)

	d := NewDrone(id, porta, addr)
	d.Iniciar(brokers)
}