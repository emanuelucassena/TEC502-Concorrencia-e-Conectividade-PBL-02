package shared

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
)

func EnviarMensagem(conn net.Conn, msg Mensagem) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem: %w", err)
	}

	size := len(data)
	header := []byte{
		byte(size >> 24),
		byte(size >> 16),
		byte(size >> 8),
		byte(size),
	}

	if _, err := conn.Write(append(header, data...)); err != nil {
		return fmt.Errorf("erro ao enviar: %w", err)
	}
	return nil
}

func ReceberMensagem(conn net.Conn) (Mensagem, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return Mensagem{}, fmt.Errorf("erro ao ler header: %w", err)
	}

	size := int(header[0])<<24 | int(header[1])<<16 |
		int(header[2])<<8 | int(header[3])

	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return Mensagem{}, fmt.Errorf("erro ao ler body: %w", err)
	}

	var msg Mensagem
	if err := json.Unmarshal(data, &msg); err != nil {
		return Mensagem{}, fmt.Errorf("erro ao deserializar: %w", err)
	}
	return msg, nil
}