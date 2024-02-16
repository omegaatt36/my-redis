package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type server struct {
	storage storage.Store
}

func main() {
	srv := &server{
		storage: storage.NewDatabase(),
	}

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

		go srv.handleConnection(conn)
	}
}

func (s *server) handleConnection(conn net.Conn) {
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

			log.Printf("Command count: %d", commandCount)

			if commandCount == 0 {
				log.Printf("Invalid command count: %d", commandCount)
				return
			}

			commands := make([]string, 0, commandCount)

			for index := 0; index < commandCount; index++ {
				// Read the command length
				if !scanner.Scan() {
					log.Println("Error reading command length")
					return
				}

				// Read the command
				if !scanner.Scan() {
					log.Println("Error reading command")
					return
				}

				log.Printf("Command: '%s'\n", scanner.Text())

				commands = append(commands, strings.Trim(scanner.Text(), "\r\n"))
			}

			log.Println("Commands: ", commands)

			response, err := s.handleCommands(strings.ToUpper(commands[0]), commands[1:]...)
			if err != nil {
				log.Println("Error handling command: ", err.Error())
				if _, err := writer.WriteString("-ERR\r\n"); err != nil {
					log.Println("Error writing to connection: ", err.Error())
					return
				}
				writer.Flush()
				return
			}

			if response == "" {
				log.Println("Empty response")
				return
			}

			log.Println("response: ", response)
			log.Println("err: ", err)

			if _, err := writer.WriteString(response); err != nil {
				log.Println("Error writing to connection: ", err.Error())
				return
			}

			writer.Flush()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading from connection: ", err.Error())
	}
}

func (s *server) handleCommands(command string, args ...string) (string, error) {
	var (
		response string
		err      error
	)

	switch command {
	case "PING":
		response = s.PING()
	case "ECHO":
		if len(args) == 0 {
			return "", fmt.Errorf("ECHO command requires a value")
		}
		response = s.ECHO(args[0])
	case "GET":
		if len(args) == 0 {
			return "", fmt.Errorf("GET command requires a key")
		}
		response, err = s.GET(args[0])
	case "SET":
		if len(args) < 2 {
			return "", fmt.Errorf("SET command requires a key and value")
		}
		response, err = s.SET(args[0], args[1], args[2:]...)
	default:
	}

	return response, err
}

// PING returns a PONG response.
func (s *server) PING() string {
	return "+PONG\r\n"
}

// ECHO returns the value passed to it.
func (s *server) ECHO(value string) string {
	return "+" + value + "\r\n"
}

// GET returns the value for a given key.
func (s *server) GET(key string) (string, error) {
	value, err := s.storage.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return "$-1\r\n", nil
		}

		return "", err
	}

	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n", nil
}

// SET sets a value for a given key.
func (s *server) SET(key string, value string, args ...string) (string, error) {

	var expiration int64
	var expirationType string

	for index := 0; index < len(args); index++ {
		if exp, err := strconv.Atoi(args[index]); err == nil {
			expiration = int64(exp)
		}
		if strings.ToUpper(args[index]) == "EX" {
			expirationType = "EX"
		} else if strings.ToUpper(args[index]) == "PX" {
			expirationType = "PX"
		}
	}
	now := time.Now().In(time.UTC)
	var expiredAt *time.Time
	if expiration != 0 {
		var t time.Time
		switch expirationType {
		case "EX":
			t = now.Add(time.Duration(expiration) * time.Second)
		case "PX":
			t = now.Add(time.Duration(expiration) * time.Millisecond * 1)
		default:
			t = now.Add(time.Duration(expiration) * time.Second)
		}

		expiredAt = &t
	}

	if err := s.storage.Set(key, value, expiredAt); err != nil {
		return "", err
	}

	return "+OK\r\n", nil
}
