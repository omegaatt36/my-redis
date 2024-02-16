package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "*") {
			commandCount, err := strconv.Atoi(strings.Trim(line, "*\r\n"))
			if err != nil {
				log.Printf("Invalid command count: %s", line)
				return
			}

			if commandCount == 1 {
				if !scanner.Scan() {
					log.Println("Error reading command length")
					return
				}

				if !scanner.Scan() {
					log.Println("Error reading command")
					return
				}

				command := strings.ToUpper(strings.Trim(scanner.Text(), "\r\n"))
				if command == "PING" {

					log.Printf("PING\n")

					_, err := writer.WriteString("+PONG\r\n")
					if err != nil {
						log.Println("Error writing to connection: ", err.Error())
						return
					}
					writer.Flush()
				}
			} else if commandCount == 2 {
				if !scanner.Scan() {
					log.Println("Error reading command length")
					return
				}

				if !scanner.Scan() {
					log.Println("Error reading command")
					return
				}

				command := strings.ToUpper(strings.Trim(scanner.Text(), "\r\n"))
				if command == "ECHO" {
					if !scanner.Scan() {
						log.Println("Error reading value length")
						return
					}

					if !scanner.Scan() {
						log.Println("Error reading value")
						return
					}

					value := strings.Trim(scanner.Text(), "\r\n")

					log.Printf("ECHO %s\n", value)

					_, err := writer.WriteString("+" + value + "\r\n")
					if err != nil {
						log.Println("Error writing to connection: ", err.Error())
						return
					}
					writer.Flush()
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading from connection: ", err.Error())
	}
}
