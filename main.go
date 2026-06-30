package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type NodeConfig struct {
	ID          int    `json:"id"`
	Host        string `json:"host"`
	ClientPort  string `json:"clientPort"`
	ClusterPort string `json:"clusterPort"`
}

type Config struct {
	Nodes []NodeConfig `json:"nodes"`
}

func loadConfig() Config {
	data, _ := os.ReadFile("config.json")
	var config Config
	json.Unmarshal(data, &config)
	return config
}

func Send_To_Node(database *Store, nodeID int, data string, parsed_data []string, msg Cluster_Message) (string, error) {
	if nodeID == database.id {
		return database.operation(parsed_data), nil
	}
	err := database.Cluster_Send(database.peers[nodeID], msg)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 1024)
	n, _ := database.peers[nodeID].Read(buf)
	var cl_msg Cluster_Message
	json.Unmarshal(buf[:n], &cl_msg)
	return cl_msg.Data, nil

}

func handleClient(conn net.Conn, database *Store) {
	defer conn.Close()
	fmt.Printf("Connected by %s\n", conn.RemoteAddr().String())
	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client %s disconnected.\n", conn.RemoteAddr().String())
			break
		}

		data := string(buffer[:n])
		parsed_data := parse_rep(data)
		valid := valid_command(parsed_data)
		switch valid {
		case true:
			shard_ID := database.ring.GetNodes(parsed_data[1], 2)
			fmt.Println("parsed_data:", parsed_data)
			fmt.Println("shard:", shard_ID)
			switch shard_ID.Leader {
			case database.id:
				respose := database.operation(parsed_data)

				switch parsed_data[0] {
				case "SET", "DEL", "EXPIRE":
					for _, Follow_nodes := range shard_ID.Followers {
						Send_To_Node(database, Follow_nodes, data, parsed_data, Cluster_Message{Type: "forward", Data: data})
					}
				}
				conn.Write([]byte(respose))

			default:
				switch parsed_data[0] {
				case "SET", "DEL", "EXPIRE":
					response, _ := Send_To_Node(database, shard_ID.Leader, data, parsed_data, Cluster_Message{Type: "replication", Data: data, Shard: shard_ID})
					conn.Write([]byte(response))

				default:
					if !database.alive_Nodes[shard_ID.Leader] {
						response_follower, _ := Send_To_Node(database, shard_ID.Followers[0], data, parsed_data, Cluster_Message{Type: "forward", Data: data})
						conn.Write([]byte(response_follower))
					} else {
						response_leader, err := Send_To_Node(database, shard_ID.Leader, data, parsed_data, Cluster_Message{Type: "forward", Data: data})
						if err != nil {
							response_follower, _ := Send_To_Node(database, shard_ID.Followers[0], data, parsed_data, Cluster_Message{Type: "forward", Data: data})
							conn.Write([]byte(response_follower))

						} else {
							conn.Write([]byte(response_leader))
						}
					}

				}

			}

		default:
			conn.Write([]byte(format_resp("Invalid Command, Try Agian", true)))

		}
	}
}

func main() {

	config := loadConfig()
	id, _ := strconv.Atoi(os.Args[1])
	node := config.Nodes[id]

	database := &Store{id: node.ID, db: &LRUCache{capacity: 4, l: list.New(), m: make(map[string]*list.Element)}, expiry: make(map[string]time.Time), peers: make(map[int]net.Conn), ring: &HashRing{points: make([]uint32, 0), owners: make(map[uint32]int)}, alive_Nodes: make(map[int]bool), heart_beat: make(map[int]time.Time)}
	database.ring.Addnode(0, 150)
	database.ring.Addnode(1, 150)
	database.ring.Addnode(2, 150)

	listener, err := net.Listen("tcp", node.Host+":"+node.ClientPort)
	if err != nil {
		fmt.Printf("Failed to bind to port: %v\n", err)
		return
	}
	defer listener.Close()
	database.LoadWAL()

	go database.ClusterRun(node.ID, config)
	time.Sleep(1 * time.Second)
	database.ConnecttoNodes(node.ID, config)

	go database.Send_heartbeat(6 * time.Second)

	go database.Check_heartbeat(15*time.Second, 4*time.Second)

	fmt.Printf("Redis Go server listening on %v:%v...\n", node.Host, node.ClientPort)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleClient(conn, database)
	}
}
