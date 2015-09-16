package main

import (
	"bufio"
	"fmt"
	//zmq "github.com/alecthomas/gozmq"
	//zmq "./extlib/zmq3"
	zmq "github.com/pebbe/zmq3"
	"os"
	"strings"
)

var address string = "tcp://127.0.0.1:5000"

var receivers map[string]int = make(map[string]int)

func startReceiver(topic string, id int) *zmq.Socket {

	zmqSocket, _ := zmq.NewSocket(zmq.SUB)
	defer zmqSocket.Close()

	zmqSocket.Connect(address)

	zmqSocket.SetSubscribe(topic)

	fmt.Println("Receiver", id, "subscribed topic", topic)

	for {

		zmqSocket.RecvBytes(0) //topic frame
		rawMsg, _ := zmqSocket.RecvBytes(0)

		msg := string(rawMsg)

		if msg == "exit" {

			fmt.Println("Closing Receiver", id)
			break
		} else {

			fmt.Println("Receiver", id, "on topic", topic, ":", msg)

		}

	}

	defer fmt.Println("Receiver", id, "closed")

	return zmqSocket
}

func main() {

	zmqPublisher, _ := zmq.NewSocket(zmq.PUB)
	defer zmqPublisher.Close()

	zmqPublisher.Bind(address)

	fmt.Println("Starting app")

	console := bufio.NewReader(os.Stdin)

	var str string
	running := true
	for running {

		str, _ = console.ReadString('\n')
		str = strings.TrimSpace(str)

		if len(str) <= 2 {

			continue
		}

		switch {

		case str[0:2] == "t:":

			topic := str[2:]

			if topic == "t" {

				fmt.Println("Please use another topic because t is reserved to issue subscribe");

				continue;

			}

			receivers[topic]++

			go startReceiver(topic, receivers[topic])

		case str == "exit":

			fmt.Println("Exiting app")
			running = false

		default:

			if separatorIdx := strings.Index(str, ":"); len(str) >= 3 && separatorIdx > 0 {

				topic := str[:separatorIdx]
				msg := str[(separatorIdx + 1):]

				fmt.Println("Sending to topic:", topic, "message:", msg)

				zmqPublisher.SendBytes([]byte(topic), zmq.SNDMORE)
				zmqPublisher.SendBytes([]byte(msg), 0)
			}

		}

	}

}
