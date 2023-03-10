package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"time"
)

func uploadFile(client RPCClient, metaData *FileMetaData, blockHashes []string) error {
	filepath := client.BaseDir + "/" + metaData.Filename
	var version int32
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		err = client.UpdateFile(metaData, &version)
		if err != nil {
			log.Fatal(err)
		}
		metaData.Version = version
		return err
	}

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var blockStoreMap map[string][]string
	if err := client.GetBlockStoreMap(blockHashes, &blockStoreMap); err != nil {
		log.Fatal(err)
	}
	hashToAddr := make(map[string]string)
	for addr, hashes := range blockStoreMap {
		for _, hash := range hashes {
			hashToAddr[hash] = addr
		}
	}
	fileStat, _ := os.Stat(filepath)
	numBlocks := int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, client.BlockSize)
		n, err := file.Read(blockData)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		blockData = blockData[:n]

		block := Block{BlockData: blockData, BlockSize: int32(n)}

		var success bool
		if err := client.PutBlock(&block, hashToAddr[GetBlockHashString(blockData)], &success); err != nil {
			log.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := client.UpdateFile(metaData, &version); err != nil {
		log.Fatal(err)
		metaData.Version = -1
	}
	metaData.Version = version

	return nil
}

func downloadFile(client RPCClient, local *FileMetaData, remote *FileMetaData) error {
	fmt.Println("in downloadFile")
	filepath := client.BaseDir + "/" + remote.Filename
	file, err := os.Create(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	*local = *remote

	//File deleted in server
	if len(remote.BlockHashList) == 1 && remote.BlockHashList[0] == "0" {
		if err := os.Remove(filepath); err != nil {
			log.Fatal(err)
			return err
		}
		return nil
	}

	var blockStoreMap map[string][]string
	if err := client.GetBlockStoreMap(remote.BlockHashList, &blockStoreMap); err != nil {
		log.Fatal(err)
	}
	hashToAddr := make(map[string]string)
	for addr, hashes := range blockStoreMap {
		for _, hash := range hashes {
			hashToAddr[hash] = addr
		}
	}

	data := ""
	for _, hash := range remote.BlockHashList {
		var block Block
		if err := client.GetBlock(hash, hashToAddr[hash], &block); err != nil {
			log.Fatal(err)
		}

		data += string(block.BlockData)
	}
	file.WriteString(data)

	return nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("test", len(localIndex))

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}
	hashMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() != "index.db" {
			blocks := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
			f, err := os.Open(client.BaseDir + "/" + file.Name())
			if err != nil {
				log.Fatal(err)
			}
			for i := 0; i < blocks; i++ {
				blockData := make([]byte, client.BlockSize)
				n, err := f.Read(blockData)
				if err != nil {
					log.Fatal(err)
				}
				blockData = blockData[:n]
				hash := GetBlockHashString(blockData)
				hashMap[file.Name()] = append(hashMap[file.Name()], hash)
			}

			if metaData, ok := localIndex[file.Name()]; ok {
				if !reflect.DeepEqual(hashMap[file.Name()], metaData.BlockHashList) {
					localIndex[file.Name()].BlockHashList = hashMap[file.Name()]
					localIndex[file.Name()].Version++
				}
			} else {
				localIndex[file.Name()] = &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: hashMap[file.Name()]}
			}
		}
	}

	for filename, metaData := range localIndex {
		if _, ok := hashMap[filename]; !ok {
			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" {
				metaData.Version++
				metaData.BlockHashList = []string{"0"}
			}
		}
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		log.Fatal(err)
	}

	for fileName, local := range localIndex {
		if remote, ok := remoteIndex[fileName]; ok {
			if local.Version > remote.Version {
				uploadFile(client, local, hashMap[fileName])
			}
		} else {
			uploadFile(client, local, hashMap[fileName])
		}
	}

	for filename, remote := range remoteIndex {
		if local, ok := localIndex[filename]; ok {
			if local.Version < remote.Version {
				downloadFile(client, local, remote)
			} else if local.Version == remote.Version && !reflect.DeepEqual(local.BlockHashList, remote.BlockHashList) {
				downloadFile(client, local, remote)
			}
		} else {
			localIndex[filename] = &FileMetaData{}
			localMetaData := localIndex[filename]
			downloadFile(client, localMetaData, remote)
		}
	}

	WriteMetaFile(localIndex, client.BaseDir)
}
