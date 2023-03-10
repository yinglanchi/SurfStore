package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	blockHash := c.Hash(blockId)
	for key := range c.ServerMap {
		if key >= blockHash {
			return c.ServerMap[key]
		}
	}
	for key := range c.ServerMap {
		return c.ServerMap[key]
	}
	panic("no servers available")

}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := ConsistentHashRing{
		ServerMap: make(map[string]string),
	}
	for _, value := range serverAddrs {
		hash := c.Hash(value)
		c.ServerMap[hash] = value
	}
	return &c
}
