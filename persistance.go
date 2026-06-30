package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"bufio"
)

func (s *Store) Send_heartbeat(send_frequency time.Duration) {
	for {
		time.Sleep(send_frequency)
		for _, peer_conn := range s.peers {
			s.Cluster_Send(peer_conn, Cluster_Message{Type: "heartbeat", SenderID: s.id})
		}
	}
}
func (s *Store) Check_heartbeat(threshold time.Duration, check_frequency time.Duration) {
	for {
		time.Sleep(check_frequency)
		s.mu.Lock()
		for peer_id, _ := range s.peers {
			if time.Since(s.heart_beat[peer_id]) > threshold {
				s.alive_Nodes[peer_id] = false
			} else {
				s.alive_Nodes[peer_id] = true
			}
		}
		s.mu.Unlock()

	}
}
func (s *Store) AppendWAL(command []string) {
	data, _ := json.Marshal(command)

	f, _ := os.OpenFile(fmt.Sprintf("wal%v.log", s.id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	f.Write(append(data, '\n'))

}
func (s *Store) LoadWAL() {
	file, err := os.Open(fmt.Sprintf("wal%v.log", s.id))
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		var command []string
		json.Unmarshal(line, &command)
		s.recover = true
		s.operation(command)
		s.recover = false

	}
	if err := scanner.Err(); err != nil {
		return
	}

}
func (s *Store) save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := json.Marshal(s.db.ToMap())
	os.WriteFile(fmt.Sprintf("data%v.json", s.id), data, 0644)
}

func (s *Store) load() {
	data, err := os.ReadFile(fmt.Sprintf("data%v.json", s.id))
	if err != nil {
		return
	}
	temp := make(map[string]string)
	json.Unmarshal(data, &temp)
	for k, v := range temp {
		s.db.Set(k, v)
	}

}
