package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

type Cluster_Message struct {
	Type     string    `json:"type"`
	Data     string    `json:"data"`
	Shard    ShardInfo `json:"shard_info"`
	SenderID int       `json:"sender_id"`
}

func (s *Store) ConnecttoNodes(myid int, config Config) {
	var wg sync.WaitGroup

	for _, node := range config.Nodes {
		if myid == node.ID {
			continue
		}
		wg.Add(1)
		go func(n NodeConfig) {
			defer wg.Done()
			for {
				nodeconn, err := net.Dial("tcp", n.Host+":"+n.ClusterPort)
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				s.mu.Lock()
				s.peers[n.ID] = nodeconn
				s.alive_Nodes[n.ID] = true
				s.mu.Unlock()
				break
			}
		}(node)
	}
	wg.Wait()

}

func (s *Store) Cluster_Recieve(peer_conn net.Conn) {
	defer peer_conn.Close()
	fmt.Printf("Connected Peer by %s\n", peer_conn.RemoteAddr().String())
	buffer := make([]byte, 1024)
	for {
		n, err := peer_conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client %s disconnected.\n", peer_conn.RemoteAddr().String())
			break
		}
		data := buffer[:n]
		var decoded_data Cluster_Message
		json.Unmarshal(data, &decoded_data)
		switch decoded_data.Type {
		case "forward":
			parsed_data := parse_rep(decoded_data.Data)
			response := s.operation(parsed_data)
			cluster_response := Cluster_Message{Type: "Response", Data: response}
			s.Cluster_Send(peer_conn, cluster_response)

		case "replication":
			parsed_data := parse_rep(decoded_data.Data)
			response := s.operation(parsed_data)
			for _, Follow_nodes := range decoded_data.Shard.Followers {
				cluster_data := Cluster_Message{Type: "forward", Data: decoded_data.Data}
				s.Cluster_Send(s.peers[Follow_nodes], cluster_data)

				buf := make([]byte, 1024)
				n, _ := s.peers[Follow_nodes].Read(buf)
				var msg Cluster_Message
				json.Unmarshal(buf[:n], &msg)

			}
			cluster_response := Cluster_Message{Type: "Response", Data: response}
			s.Cluster_Send(peer_conn, cluster_response)
		case "heartbeat":
			s.mu.Lock()
			s.heart_beat[decoded_data.SenderID] = time.Now()
			s.mu.Unlock()
		default:
			fmt.Println(decoded_data.Data)
		}

	}
}

func (s *Store) Cluster_Send(send_conn net.Conn, message Cluster_Message) error {
	json_data, ok := json.Marshal(message)
	if ok != nil {
		fmt.Println("Error writing data:", ok)
		return ok
	}
	bytesWritten, err := send_conn.Write(json_data)
	if err != nil {
		fmt.Println("Error writing data:", err)
		return err
	}

	fmt.Printf("Successfully sent %d bytes.\n", bytesWritten)
	return nil
}

func (s *Store) ClusterRun(myid int, config Config) {
	node := config.Nodes[myid]
	cluster_listener, err := net.Listen("tcp", node.Host+":"+node.ClusterPort)

	if err != nil {
		fmt.Printf("Failed to bind to port: %v\n", err)
		return
	}
	defer cluster_listener.Close()
	for {
		peer_conn, err := cluster_listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go s.Cluster_Recieve(peer_conn)
	}
}
