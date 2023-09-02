package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/odedro987/go-pubsub/pkg/maps"
	"github.com/odedro987/go-pubsub/pkg/pubsub"
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

type PublishMessage struct {
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
}

type SubscribeMessage struct {
	Topic  string `json:"topic"`
	Client net.Conn
}

type Server struct {
	info     Info
	listener net.Listener

	subscriptionQueue chan SubscribeMessage
	queue             chan PublishMessage
	topics            maps.SyncMap[chan PublishMessage]
	clients           maps.SyncMap[map[string]net.Conn]
}

func New(info Info) (*Server, error) {
	server := &Server{
		info:              info,
		queue:             make(chan PublishMessage, 1000),
		subscriptionQueue: make(chan SubscribeMessage, 1000),
		topics:            maps.New[chan PublishMessage](),
		clients:           maps.New[map[string]net.Conn](),
	}

	l, err := net.Listen(string(info.ConnectionType), info.Address())
	if err != nil {
		return nil, err
	}
	server.listener = l

	return server, nil
}

func (s *Server) StartAccepting() {
	log.Println("Start accepting connections")
	defer func() {
		s.clients.Range(func(topicName string, clientsMap map[string]net.Conn) bool {
			for _, client := range clientsMap {
				client.Close()
			}
			return true
		})
	}()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Fatal(err)
			return
		}

		go func(c net.Conn) {
			defer c.Close()
			for {
				bytes, msgType, err := pubsub.ReadMessage(c)
				if err != nil {
					log.Println("Closed", c.RemoteAddr().String(), err)
					break
				}
				switch msgType {
				case pubsub.PublishMessage:
					var msg PublishMessage
					err = json.Unmarshal(bytes, &msg)
					if err != nil {
						log.Println(err.Error())
						continue
					}
					s.queue <- msg
				case pubsub.SubscribeMessage:
					var msg SubscribeMessage
					err = json.Unmarshal(bytes, &msg)
					if err != nil {
						log.Println(err.Error())
						continue
					}
					msg.Client = c
					s.subscriptionQueue <- msg
				}
			}
		}(conn)
	}
}

func (s *Server) addTopic(name string) {
	_, ok := s.topics.Load(name)
	if ok {
		return
	}
	log.Println("Adding new topic: " + name)

	topicChannel := make(chan PublishMessage)
	s.topics.Store(name, topicChannel)
	go func() {
		for {
			select {
			case message, ok := <-topicChannel:
				if !ok {
					log.Println("Channel is closed. Exiting.")
					return
				}
				log.Printf("Topic: %s -> %v", name, message)
				clientsMap, ok := s.clients.Load(name)
				if !ok {
					log.Printf("No subscribers in topic: %s\n", name)
					continue
				}
				for _, client := range clientsMap {
					err := pubsub.SendMessage(client, message, pubsub.PublishMessage)
					if err != nil {
						log.Printf("Error sending message to client %s: %s", client.RemoteAddr().String(), err)
					}
					log.Printf("Sending to client %s from topic %s: %s", client.RemoteAddr().String(), name, message.Data)
				}
			}
		}
	}()
}

func (s *Server) addSubscriber(topic string, client net.Conn) {
	_, ok := s.clients.Load(topic)
	if !ok {
		s.clients.Store(topic, make(map[string]net.Conn))
	}
	clientsMap, _ := s.clients.Load(topic)
	_, ok = clientsMap[client.RemoteAddr().String()]
	if ok {
		return
	}

	log.Printf("Subscribing %s to topic: %s\n", client.RemoteAddr().String(), topic)

	clientsMap[client.RemoteAddr().String()] = client
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
				topicChannel, ok := s.topics.Load(message.Topic)
				if !ok {
					return
				}
				topicChannel <- message
			}()
		case message, ok := <-s.subscriptionQueue:
			if !ok {
				log.Println("Channel is closed. Exiting.")
				return
			}
			go func() {
				s.addSubscriber(message.Topic, message.Client)
			}()

		}
	}

}
