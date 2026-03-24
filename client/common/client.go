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
	ID             uint32
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	MaxBatchAmount int
	Nombre         string
	Apellido       string
	Documento      uint64
	Nacimiento     string
	Numero         uint32
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

type Bet struct {
	Nombre     string
	Apellido   string
	Document   uint64
	Nacimiento string
	Numero     uint32
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
	bets, err := readsBetsFromCSV("agency.csv")
	if err != nil {
		log.Criticalf("action: read_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}

	batches := splitBetsIntoBatches(bets, c.config.MaxBatchAmount, 8*1024)

	if err := c.createClientSocket(); err != nil {
		log.Criticalf("action: create_socket | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
	defer c.conn.Close()

	log.Criticalf("action: create_socket | result: success | client_id: %v ", c.config.ID)

	for i, batch := range batches {
		if len(SignalChannel) > 0 {
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		}

		message, err := buildBatchMessage(c.config.ID, batch)
		if err != nil {
			log.Criticalf("action: build_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
		}

		err = writeFully(c.conn, message)
		if err != nil {
			log.Criticalf("action: write_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
		}

		ackID, ackStatus, err := ReadACK(c.conn)
		if err != nil {
			log.Criticalf("action: read_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		}

		if ackID != c.config.ID {
			log.Errorf("action: read_ack | result: fail | client_id: %v | error: ACK ID mismatch (expected %v, got %v)", c.config.ID, c.config.ID, ackID)
		}

		if ackStatus != 0x00 {
			log.Errorf("action: read_ack | result: fail | client_id: %v | error: ACK status indicates failure (status code: %v)", c.config.ID, ackStatus)
		}

		log.Infof("action: batch_sent | result: success | client_id: %v | batch_n: %v | cantidad: %v | ack_status: %v", c.config.ID, i, len(batch), ackStatus)
	}

	finishedMessage := buildFinishedMessage(c.config.ID)
	if err := writeFully(c.conn, finishedMessage); err != nil {
		log.Criticalf("action: send_fin | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}

	_, _, err = ReadACK(c.conn)
	if err != nil {
		log.Criticalf("action: recv_fin_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		c.conn.Close()
		return
	}

	log.Infof("action: fin_apuestas | result: success | client_id: %v", c.config.ID)
	c.conn.Close()

	for {
		time.Sleep(c.config.LoopPeriod)

		if err := c.createClientSocket(); err != nil {
			return
		}

		queryMessage := buildQueryMessage(c.config.ID)
		if err := writeFully(c.conn, queryMessage); err != nil {
			log.Errorf("action: send_query | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			continue
		}

		winners, ready, err := ReadWinnersResponse(c.conn)
		c.conn.Close()

		if err != nil {
			log.Errorf("action: recv_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		if !ready {
			log.Infof("action: consulta_ganadores | result: pending | client_id: %v", c.config.ID)
			continue
		}

		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", len(winners))
		return
	}
}
