package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

type ConnectionType string

const (
	TCP ConnectionType = "tcp"
)

type Info struct {
	ConnectionType ConnectionType
	Port           int32
	Host           string
}

func (i Info) Address() string {
	return fmt.Sprintf("%s:%d", i.Host, i.Port)
}

type InboundMessage struct {
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
}

type Server struct {
	info     Info
	listener net.Listener

	queue  chan InboundMessage
	topics map[string]chan interface{}
}

func New(info Info) (*Server, error) {
	server := &Server{
		info:   info,
		queue:  make(chan InboundMessage, 1000),
		topics: make(map[string]chan interface{}),
	}

	l, err := net.Listen(string(info.ConnectionType), info.Address())
	if err != nil {
		return nil, err
	}
	server.listener = l

	return server, nil
}

func readMessage(conn *net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	_, err := (*conn).Read(header)
	if err != nil {
		return nil, err
	}
	messageLength := binary.LittleEndian.Uint32(header)

	buffer := make([]byte, messageLength)
	_, err = (*conn).Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (s *Server) StartAccepting() {
	log.Println("Start accepting connections")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Fatal(err)
			return
		}

		go func(c net.Conn) {
			defer c.Close()
			for {
				bytes, err := readMessage(&conn)
				if err != nil {
					log.Println("Closed", c.RemoteAddr().String())
					break
				}
				var msg InboundMessage
				err = json.Unmarshal(bytes, &msg)
				if err != nil {
					log.Println(err.Error())
					continue
				}
				s.queue <- msg
			}
		}(conn)
	}
}

func (s *Server) addTopic(name string) {
	_, ok := s.topics[name]
	if ok {
		return
	}
	log.Println("Adding new topic: " + name)

	s.topics[name] = make(chan interface{})
	go func() {
		for {
			select {
			case message, ok := <-s.topics[name]:
				if !ok {
					log.Println("Channel is closed. Exiting.")
					return
				}
				log.Printf("Topic: %s -> %v", name, message)
			}
		}
	}()
}

func (s *Server) StartQueuing() {
	log.Println("Start queuing messages")
	for {
		select {
		case message, ok := <-s.queue:
			if !ok {
				log.Println("Channel is closed. Exiting.")
				return
			}
			go func() {
				log.Println("Queuing message to " + message.Topic)
				s.addTopic(message.Topic)
				s.topics[message.Topic] <- message.Data
			}()
		}
	}

}
