package common

import (
	"io"
	"net"
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
