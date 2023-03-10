package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	index := 0
	for _, fileMeta := range fileMetas {
		fileName := fileMeta.Filename
		version := fileMeta.Version
		hashIndex := index
		for _, hashValue := range fileMeta.BlockHashList {
			_, err = db.Exec(`INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?)`, fileName, version, hashIndex, hashValue)
			if err != nil {
				log.Fatal(err)
			}
		}
		index++
	}
	db.Close()
	return nil
}

/*
Reading Local Metadata File Related
*/

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	fileMetaMap = make(map[string]*FileMetaData)
	metaFilePath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	fs, err := os.Stat(metaFilePath)
	if err != nil || fs == nil {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal(err)
	}
	var filename string
	var version int
	var hashIndex int
	var hashValue string
	var hashValueSlices []string
	rows, err := db.Query("SELECT * FROM indexes")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	for rows.Next() {
		fmt.Println(rows)
		err := rows.Scan(&filename, &version, &hashIndex, &hashValue)
		if err != nil {
			log.Fatal(err)
		}
		if _, ok := fileMetaMap[filename]; ok {
			hashValueSlices = append(hashValueSlices, strings.Split(hashValue, " ")...)
		} else {
			hashValueSlices = strings.Split(hashValue, " ")
		}
		fileMetaMap[filename] = &FileMetaData{Filename: filename, Version: int32(version), BlockHashList: hashValueSlices}
	}
	db.Close()
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
