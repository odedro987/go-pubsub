package client

import (
	"encoding/json"
	"log"
	"net"

	"github.com/odedro987/go-pubsub/pkg/pubsub"
	"github.com/odedro987/go-pubsub/pkg/server"
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

func (c *Client) ConnectionClosed() bool {
	return !c.connected
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

	payload := server.PublishMessage{
		Topic: topic,
		Data:  message,
	}

	err := pubsub.SendMessage(c.conn, payload, pubsub.PublishMessage)
	if err != nil {
		c.connected = false
		return err
	}

	return nil
}

func (c *Client) Subscribe(topics []string, handler func(msg *server.PublishMessage)) error {
	if !c.connected {
		return nil
	}

	for _, topic := range topics {
		payload := server.SubscribeMessage{
			Topic: topic,
		}

		err := pubsub.SendMessage(c.conn, payload, pubsub.SubscribeMessage)
		if err != nil {
			c.connected = false
			return err
		}
	}

	go func(conn net.Conn) {
		log.Println("Start accepting messages")
		for {
			defer conn.Close()
			for {
				bytes, _, err := pubsub.ReadMessage(conn)
				if err != nil {
					log.Println("Closed", conn.RemoteAddr().String())
					c.connected = false
					break
				}
				var msg server.PublishMessage
				err = json.Unmarshal(bytes, &msg)
				if err != nil {
					log.Println(err.Error())
					continue
				}
				handler(&msg)
			}
		}
	}(c.conn)

	return nil
}
