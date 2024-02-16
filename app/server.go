package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		log.Println("New connection from: ", conn.RemoteAddr().String())

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	defer fmt.Println("Connection closed")

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Text:", text)
		if scanner.Text() == "ping" {
			_, err := conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Error while writing data:", err.Error())
				return
			}
		}
	}
}
