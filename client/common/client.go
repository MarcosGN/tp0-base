package common

import (
	"bufio"
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
	nombreLength := uint32(len(nombre))
	nombreLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint32(nombreLengthBytes, nombreLength)
	payload = append(payload, nombreLengthBytes...)
	payload = append(payload, nombreBytes...)

	apellidoBytes := []byte(apellido)
	apellidoLength := uint32(len(apellido))
	apellidoLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint32(apellidoLengthBytes, apellidoLength)
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

	return payload, nil
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

		// TODO: Modify the send to avoid short-write
		fmt.Fprintf(
			c.conn,
			"[CLIENT %v] Message N°%v\n",
			c.config.ID,
			msgID,
		)
		msg, err := bufio.NewReader(c.conn).ReadString('\n')
		c.conn.Close()

		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
			c.config.ID,
			msg,
		)

		// Wait a time between sending one message and the next one
		time.Sleep(c.config.LoopPeriod)

	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
