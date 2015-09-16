package main

import (
	"bufio"
	"fmt"
	zmq "github.com/pebbe/zmq3"
	"os"
	"strings"
)

var address string = "tcp://127.0.0.1:5000"

func startReceiver(identity string) *zmq.Socket {

	zmqSocket, _ := zmq.NewSocket(zmq.DEALER)
	defer zmqSocket.Close()

	zmqSocket.SetIdentity(identity)

	zmqSocket.Connect(address)

	fmt.Println("Receiver", identity, "started")

	for {

		rawMsg, _ := zmqSocket.RecvBytes(0)

		msg := string(rawMsg)

		if msg == "exit" {

			fmt.Println("Closing Receiver", identity)
			break
		} else {

			fmt.Println("Receiver", identity, ":", msg)

		}

	}

	defer fmt.Println("Receiver", identity, "closed")

	return zmqSocket
}

func main() {

	zmqRouter, _ := zmq.NewSocket(zmq.ROUTER)
	defer zmqRouter.Close()

	zmqRouter.Bind(address)

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

		case str[0:2] == "c:":

			go startReceiver(str[2:])

		case str == "exit":

			fmt.Println("Exiting app")
			running = false

		default:

			if separatorIdx := strings.Index(str, ":"); len(str) >= 3 && separatorIdx > 0 {

				identity := str[:separatorIdx]
				msg := str[(separatorIdx + 1):]

				//fmt.Println("Sencding to receiver:", identity, "message:", msg)

				zmqRouter.SendBytes([]byte(identity), zmq.SNDMORE)
				zmqRouter.SendBytes([]byte(msg), 0)
			}

		}

		zmqRouter.Recv(zmq.DONTWAIT) //hack to fix zmq identity reconnect issues

	}

}
