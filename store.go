package main

import (
	"container/list"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"
)

type LRUCache struct {
	capacity int
	l        *list.List
	m        map[string]*list.Element
}
type entry struct {
	key   string
	value string
}

func (dict *LRUCache) Get(key string) (string, bool) {
	val, ok := dict.m[key]
	if ok {
		dict.l.MoveToBack(val)
		return val.Value.(*entry).value, true
	}
	return "", false
}
func (dict *LRUCache) Set(key string, value string) bool {
	val, ok := dict.m[key]
	if ok {
		dict.l.MoveToBack(val)
		val.Value.(*entry).value = value
		return true
	}
	dict.m[key] = dict.l.PushBack(&entry{key, value})
	if dict.capacity < dict.l.Len() {
		delete(dict.m, dict.l.Front().Value.(*entry).key)
		dict.l.Remove(dict.l.Front())
	}
	return true
}
func (dict *LRUCache) Del(key string) bool {
	val, ok := dict.m[key]
	if ok {
		dict.l.Remove(val)
		delete(dict.m, key)
		return true
	}
	return false
}

func (dict *LRUCache) ToMap() map[string]string {
	result := make(map[string]string)
	for k, v := range dict.m {
		result[k] = v.Value.(*entry).value
	}
	return result
}

type Store struct {
	mu          sync.Mutex
	db          *LRUCache
	expiry      map[string]time.Time
	peers       map[int]net.Conn
	ring        *HashRing
	id          int
	recover     bool
	heart_beat  map[int]time.Time
	alive_Nodes map[int]bool
}

func (s *Store) operation(command []string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch command[0] {
	case "SET":
		if !s.recover {
			s.AppendWAL(command)
		}
		s.db.Set(command[1], command[2])
		return format_resp("OK", false)
	case "GET":
		val, ok := s.expiry[command[1]]
		if ok {
			if time.Now().After(val) {
				s.db.Del(command[1])
				delete(s.expiry, command[1])
				return format_resp(nil, false)
			}
		}
		value, dbok := s.db.Get(command[1])
		if dbok {
			return format_resp(value, true)
		}
		return format_resp(nil, false)
	case "DEL":
		if !s.recover {
			s.AppendWAL(command)
		}
		_, ok := s.db.Get(command[1])
		s.db.Del(command[1])
		delete(s.expiry, command[1])
		if ok {
			return format_resp(1, false)
		}
		return format_resp(0, false)
	case "EXPIRE":
		if !s.recover {
			s.AppendWAL(command)
		}
		_, ok := s.db.Get(command[1])
		if ok {
			secs, _ := strconv.Atoi(command[2])
			s.expiry[command[1]] = time.Now().Add(time.Duration(secs) * time.Second)
			return format_resp(1, false)
		}
		return format_resp(0, false)
	case "TTL":
		_, ok := s.db.Get(command[1])
		if ok {
			_, ok := s.expiry[command[1]]
			if ok {
				left := int(time.Until(s.expiry[command[1]]).Seconds())
				return format_resp(left, false)
			}
			return format_resp(-1, false)
		}
		return format_resp(-2, false)

	default:
		return format_resp(errors.New("Not the right command"), false)

	}

}
