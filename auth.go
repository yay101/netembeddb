package netembeddb

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const authTimeout = 5 * time.Second

func authenticateClient(conn net.Conn, authKey string) error {
	if authKey == "" {
		return nil
	}

	conn.SetReadDeadline(time.Now().Add(authTimeout))
	defer conn.SetReadDeadline(time.Time{})

	lengthBuf := make([]byte, binary.MaxVarintLen64)
	n, err := conn.Read(lengthBuf)
	if err != nil {
		return fmt.Errorf("failed to read auth key length: %w", err)
	}

	var length uint64
	for i := 0; i < n; i++ {
		length |= uint64(lengthBuf[i]&0x7F) << (7 * i)
		if lengthBuf[i]&0x80 == 0 {
			break
		}
	}

	if length > 1024 {
		return fmt.Errorf("auth key too long")
	}

	keyBuf := make([]byte, length)
	_, err = conn.Read(keyBuf)
	if err != nil {
		return fmt.Errorf("failed to read auth key: %w", err)
	}

	if string(keyBuf) != authKey {
		return fmt.Errorf("invalid auth key")
	}

	return nil
}

func writeAuthResponse(conn net.Conn, err error) error {
	conn.SetWriteDeadline(time.Now().Add(authTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	if err != nil {
		conn.Write([]byte{0})
		binary.Write(conn, binary.BigEndian, uint32(len(err.Error())))
		conn.Write([]byte(err.Error()))
		return err
	}

	conn.Write([]byte{1})
	return nil
}

func readAuthKey(conn net.Conn) (string, error) {
	conn.SetReadDeadline(time.Now().Add(authTimeout))
	defer conn.SetReadDeadline(time.Time{})

	lengthBuf := make([]byte, binary.MaxVarintLen64)
	n, err := conn.Read(lengthBuf)
	if err != nil {
		return "", fmt.Errorf("failed to read key length: %w", err)
	}

	var length uint64
	for i := 0; i < n; i++ {
		length |= uint64(lengthBuf[i]&0x7F) << (7 * i)
		if lengthBuf[i]&0x80 == 0 {
			break
		}
	}

	if length > 1024 {
		return "", fmt.Errorf("key too long")
	}

	keyBuf := make([]byte, length)
	_, err = conn.Read(keyBuf)
	if err != nil {
		return "", fmt.Errorf("failed to read key: %w", err)
	}

	return string(keyBuf), nil
}

type fixedReader struct {
	data []byte
	pos  int
}

func (fr *fixedReader) Read(p []byte) (n int, err error) {
	if fr.pos >= len(fr.data) {
		return 0, io.EOF
	}
	n = copy(p, fr.data[fr.pos:])
	fr.pos += n
	return n, nil
}
