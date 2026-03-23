package common

import (
	"encoding/csv"
	"io"
	"net"
	"os"
	"strconv"
)

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

func readsBetsFromCSV(filePath string) ([]Bet, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var bets []Bet
	for _, line := range lines {
		if len(line) != 5 {
			continue
		}

		documento, err := strconv.ParseUint(line[2], 10, 64)
		if err != nil {
			return nil, err
		}

		numero, err := strconv.ParseUint(line[4], 10, 32)
		if err != nil {
			return nil, err
		}

		bets = append(bets, Bet{
			Nombre:     line[0],
			Apellido:   line[1],
			Document:   documento,
			Nacimiento: line[3],
			Numero:     uint32(numero),
		})
	}

	return bets, nil
}

func splitBetsIntoBatches(bets []Bet, maxAmount int, maxBytes int) [][]Bet {
	var batches [][]Bet
	var currentBatch []Bet
	currentBatchSize := 0

	for _, bet := range bets {
		betSize := 2 + len(bet.Nombre) + 2 + len(bet.Apellido) + 8 + 10 + 4

		if currentBatchSize+betSize > maxBytes || len(currentBatch) >= maxAmount {
			batches = append(batches, currentBatch)
			currentBatch = []Bet{}
			currentBatchSize = 0
		}

		currentBatch = append(currentBatch, bet)
		currentBatchSize += betSize
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}
