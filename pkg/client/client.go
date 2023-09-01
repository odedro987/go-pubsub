package client

import (
	"encoding/binary"
	"encoding/json"
	"go-pubsub/pkg/server"
	"net"
)

type Client struct {
	serverInfo server.Info
	conn       net.Conn
	connected  bool

	topics []string
}

func New() *Client {
	return &Client{}
}

func (c *Client) Connect(info server.Info) error {
	c.serverInfo = info

	var err error
	c.conn, err = net.Dial(string(server.TCP), info.Address())
	if err != nil {
		return err
	}

	c.connected = true

	return nil
}

func (c *Client) Disconnect() error {
	if !c.connected {
		return nil
	}

	err := c.conn.Close()
	if err != nil {
		return err
	}

	c.connected = false
	return nil
}

func (c *Client) Publish(topic string, message interface{}) error {
	if !c.connected {
		return nil
	}

	payload := server.InboundMessage{
		Topic: topic,
		Data:  message,
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	messageLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLength, uint32(len(bytes)))

	_, err = c.conn.Write(messageLength)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
