package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	if _, ok := m.FileMetaMap[filename]; ok {
		if version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockHashes := make(map[string][]string)
	blockStoreMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		blockServerAddr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		fmt.Println("in GetBlockStoreMap with hash: ", blockHash, "addr: ", blockServerAddr)
		blockHashes[blockServerAddr] = append(blockHashes[blockServerAddr], blockHash)
	}
	for addr, hashes := range blockHashes {
		blockStoreMap[addr] = &BlockHashes{Hashes: hashes}
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

// Returns all the BlockStore addresses
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
