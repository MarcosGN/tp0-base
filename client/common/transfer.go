package common

import (
	"encoding/binary"
	"fmt"
	"net"
)

func buildBatchMessage(id uint32, bets []Bet) ([]byte, error) {
	payload := make([]byte, 0)

	payload = append(payload, 0x03) // Message type: Batch

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)
	payload = append(payload, idBytes...)

	numBets := uint16(len(bets))
	numBetsBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(numBetsBytes, numBets)
	payload = append(payload, numBetsBytes...)

	for _, bet := range bets {
		payload = append(payload, 0x01) // message type BET

		idBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idBytes, id)
		payload = append(payload, idBytes...)

		nombreBytes := []byte(bet.Nombre)
		nombreLength := uint16(len(bet.Nombre))
		nombreLengthBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(nombreLengthBytes, nombreLength)
		payload = append(payload, nombreLengthBytes...)
		payload = append(payload, nombreBytes...)

		apellidoBytes := []byte(bet.Apellido)
		apellidoLength := uint16(len(bet.Apellido))
		apellidoLengthBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(apellidoLengthBytes, apellidoLength)
		payload = append(payload, apellidoLengthBytes...)
		payload = append(payload, apellidoBytes...)

		documentoBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(documentoBytes, bet.Document)
		payload = append(payload, documentoBytes...)

		if len(bet.Nacimiento) != 10 {
			return nil, fmt.Errorf("nacimiento debe tener formato YYYY-MM-DD")
		}
		nacimientoBytes := []byte(bet.Nacimiento)
		payload = append(payload, nacimientoBytes...)

		numeroBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(numeroBytes, bet.Numero)
		payload = append(payload, numeroBytes...)
	}

	finalMessage := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(finalMessage, uint32(len(payload)))
	copy(finalMessage[4:], payload)

	return finalMessage, nil
}

func ReadACK(conn net.Conn) (uint32, byte, error) {
	lenBytes, err := readFully(conn, 4)
	if err != nil {
		return 0, 0, err
	}

	length := binary.BigEndian.Uint32(lenBytes)

	payload, err := readFully(conn, int(length))
	if err != nil {
		return 0, 0, err
	}

	if len(payload) < 6 {
		return 0, 0, fmt.Errorf("ACK payload demasiado corto")
	}

	messageType := payload[0]
	id := binary.BigEndian.Uint32(payload[1:5])
	status := payload[5]

	if messageType != 0x02 {
		return 0, 0, fmt.Errorf("Tipo de mensaje inesperado: %v", messageType)
	}

	if status != 0x00 {
		return id, messageType, fmt.Errorf("ACK indica error para el mensaje con ID %v", id)
	}

	return id, status, nil
}
