package main

import (
    "log"
    "os"
    "strings"
)

func main() {
    id := os.Getenv("BROKER_ID")           // ex: "A"
    porta := os.Getenv("PORTA")            // ex: "5001"
    // Lista dos outros brokers: "broker-b:5002,broker-c:5003"
    peersStr := os.Getenv("PEERS")
    peers := strings.Split(peersStr, ",")

    log.Printf("[Broker-%s] iniciando na porta %s", id, porta)

    b := NewBroker(id, porta, peers)
    b.Iniciar() // bloqueia
}