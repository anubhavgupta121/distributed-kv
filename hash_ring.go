package main

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
)

type HashRing struct {
	points []uint32
	owners map[uint32]int
}
type ShardInfo struct {
	Leader    int
	Followers []int
}

func hashing(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()

}
func (hash *HashRing) Addnode(id int, virtual_count int) {
	for i := 1; i <= virtual_count; i++ {
		s := fmt.Sprintf("node-%v-replica-%v", id, i)
		hashed_node := hashing(s)
		hash.points = append(hash.points, hashed_node)
		hash.owners[hashed_node] = id
	}
	slices.Sort(hash.points)
}
func (hash *HashRing) GetNodes(key string, count int) ShardInfo {
	hashed_key := hashing(key)
	idx := sort.Search(len(hash.points), func(i int) bool {
		return hash.points[i] >= hashed_key
	})
	var shard ShardInfo

	if idx == len(hash.points) {
		idx = 0
	}
	shard.Leader = hash.owners[hash.points[idx]]
	found := make(map[int]bool)
	found[shard.Leader] = true
	fmt.Println("key:", key, "hashed:", hashed_key, "idx:", idx)

	for j := 1; j < len(hash.points); j++ {
		i := (idx + j) % len(hash.points)
		nodeID := hash.owners[hash.points[i]]
		if !found[nodeID] {
			shard.Followers = append(shard.Followers, nodeID)
			found[nodeID] = true
		}
		if len(found) == count {
			break
		}
	}
	return shard
}
