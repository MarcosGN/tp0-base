package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            uint32
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	Nombre        string
	Apellido      string
	Documento     uint64
	Nacimiento    string
	Numero        uint32
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func writeFully(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	return nil
}

func readFully(conn net.Conn, size int) ([]byte, error) {
	buffer := make([]byte, size)
	_, err := io.ReadFull(conn, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

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

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop(SignalChannel chan os.Signal) {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		if len(SignalChannel) > 0 {
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			c.conn.Close()
			return
		}
		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		msg, err := buildBetMessage(c.config.ID, c.config.Nombre, c.config.Apellido, c.config.Documento, c.config.Nacimiento, c.config.Numero)
		if err != nil {
			log.Errorf("action: build_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			c.conn.Close()
			return
		}

		err = writeFully(c.conn, msg)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			c.conn.Close()
			return
		}

		log.Infof("action: send_message | result: success | client_id: %v | msg_id: %v",
			c.config.ID,
			msgID,
		)

		ackID, ackStatus, err := ReadACK(c.conn)
		c.conn.Close()

		if err != nil {
			log.Errorf("action: read_ack | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: read_ack | result: success | client_id: %v | ack_id: %v | ack_status: %v",
			c.config.ID,
			ackID,
			ackStatus,
		)

		log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", c.config.Documento, c.config.Numero)

		// Wait a time between sending one message and the next one
		time.Sleep(c.config.LoopPeriod)

	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
