package pubsub

import (
	"encoding/binary"
	"encoding/json"
	"net"
)

type MessageType uint16

const (
	SubscribeMessage MessageType = iota
	PublishMessage
)

func SendMessage(conn net.Conn, message interface{}, msgType MessageType) error {
	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	messageLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLength, uint32(len(bytes)))

	_, err = conn.Write(messageLength)
	if err != nil {
		return err
	}

	msgTypeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(msgTypeBytes, uint16(msgType))
	_, err = conn.Write(msgTypeBytes)
	if err != nil {
		return err
	}

	_, err = conn.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func ReadMessage(conn net.Conn) ([]byte, MessageType, error) {
	lenHeader := make([]byte, 4)
	_, err := conn.Read(lenHeader)
	if err != nil {
		return nil, 0, err
	}
	messageLength := binary.LittleEndian.Uint32(lenHeader)

	typeHeader := make([]byte, 2)
	_, err = conn.Read(typeHeader)
	if err != nil {
		return nil, 0, err
	}
	msgType := binary.LittleEndian.Uint16(typeHeader)

	buffer := make([]byte, messageLength)
	_, err = conn.Read(buffer)
	if err != nil {
		return nil, 0, err
	}

	return buffer, MessageType(msgType), nil
}
