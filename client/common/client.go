package common

import (
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

// StartClientLoop Send messages to the client
func (c *Client) StartClientLoop(SignalChannel chan os.Signal) {
	if len(SignalChannel) > 0 {
		log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
		c.conn.Close()
		return
	}
	// Create the connection the server.
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

	log.Infof("action: send_message | result: success | client_id: %v |",
		c.config.ID,
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
}
