package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type ShardBlockMapping struct {
	//Header
	ShardBlockMapping map[[64]byte]int
	EpochHeight		  int
}

func NewShardBlockMapping() *ShardBlockMapping {
	newMapping := new(ShardBlockMapping)
	newMapping.ShardBlockMapping = make(map[[64]byte]int)
	newMapping.EpochHeight = 0
	return newMapping
}


func (shardBlockMapping *ShardBlockMapping) HashMapping() [32]byte {
	if shardBlockMapping == nil {
		return [32]byte{}
	}

	mappingHash := struct {
		ValMapping				  map[[64]byte]int
		EpochHeight				  int
	}{
		shardBlockMapping.ShardBlockMapping,
		shardBlockMapping.EpochHeight,
	}
	return SerializeHashContent(mappingHash)
}

func (shardBlockMapping *ShardBlockMapping) GetSize() int {
	size := len(shardBlockMapping.ShardBlockMapping)
	return size
}

func (shardBlockMapping *ShardBlockMapping) Encode() []byte {
	if shardBlockMapping == nil {
		return nil
	}

	encoded := ShardBlockMapping{
		ShardBlockMapping:                shardBlockMapping.ShardBlockMapping,
		EpochHeight:			   shardBlockMapping.EpochHeight,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (shardBlockMapping *ShardBlockMapping) Decode(encoded []byte) (shardBlockMappingDecided *ShardBlockMapping) {
	if encoded == nil {
		return nil
	}

	var decoded ShardBlockMapping
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (shardBlockMapping ShardBlockMapping) String() string {
	mappingString := "\n"
	for k, v := range shardBlockMapping.ShardBlockMapping {
		mappingString += fmt.Sprintf("Entry: %x -> %v\n", k[:8],v)
	}
	mappingString += fmt.Sprintf("Epoch Height: %d\n", shardBlockMapping.EpochHeight)
	return mappingString
}
