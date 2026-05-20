package main

import (
	"log"
	"os"
	"strconv"
)

func main() {
	setorID := os.Getenv("SETOR_ID")
	brokerAddr := os.Getenv("BROKER_ADDR")
	intervalo, _ := strconv.Atoi(os.Getenv("INTERVALO_MS"))
	if intervalo == 0 {
		intervalo = 2000
	}

	log.Printf("[Sensor] iniciando no setor %s → %s", setorID, brokerAddr)

	s := NewSensor(setorID, brokerAddr, intervalo)
	s.Iniciar()
}