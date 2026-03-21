package common

import (
	"encoding/binary"
	"fmt"
	"net"
)

func buildBetMessage(id uint32,
	nombre string,
	apellido string,
	documento uint64,
	nacimiento string,
	numero uint32) ([]byte, error) {
	payload := make([]byte, 0)

	payload = append(payload, 0x01) // Message type: Bet

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)
	payload = append(payload, idBytes...)

	nombreBytes := []byte(nombre)
	nombreLength := uint16(len(nombre))
	nombreLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nombreLengthBytes, nombreLength)
	payload = append(payload, nombreLengthBytes...)
	payload = append(payload, nombreBytes...)

	apellidoBytes := []byte(apellido)
	apellidoLength := uint16(len(apellido))
	apellidoLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(apellidoLengthBytes, apellidoLength)
	payload = append(payload, apellidoLengthBytes...)
	payload = append(payload, apellidoBytes...)

	documentoBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(documentoBytes, documento)
	payload = append(payload, documentoBytes...)

	if len(nacimiento) != 10 {
		return nil, fmt.Errorf("nacimiento debe tener formato YYYY-MM-DD")
	}
	nacimientoBytes := []byte(nacimiento)
	payload = append(payload, nacimientoBytes...)

	numeroBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(numeroBytes, numero)
	payload = append(payload, numeroBytes...)

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
