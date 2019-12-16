package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
)

type ShardBlock struct {
	//Header
	Header       		byte
	Hash         		[32]byte
	PrevBlockHash     	[32]byte
	Height       		uint32

	//Body
	Timestamp             int64
	MerkleRoot            [32]byte
	MerklePatriciaRoot    [32]byte
	CommitmentProof       [crypto.COMM_PROOF_LENGTH]byte
	ShardState			  map[[32]byte]*Account
	ValMapping			  *ValShardMapping
	NofMiners			  int
}

func NewShardBlock(prevBlockHash [32]byte, height uint32) *ShardBlock {
	newShardBlock := ShardBlock{
		PrevBlockHash: prevBlockHash,
		Height:   height,
	}

	return &newShardBlock
}

func (shardBlock *ShardBlock) HashShardBlock() [32]byte {
	if shardBlock == nil {
		return [32]byte{}
	}

	blockHash := struct {
		prevShardHashes               [32]byte
		timestamp             		  int64
		merkleRoot            		  [32]byte
		merklePatriciaRoot	  		  [32]byte
		height				  		  uint32
		commitmentProof       		  [crypto.COMM_PROOF_LENGTH]byte
		shardState					  map[[32]byte]*Account
		valMapping					  *ValShardMapping
		noMiners					  int
	}{
		shardBlock.PrevBlockHash,
		shardBlock.Timestamp,
		shardBlock.MerkleRoot,
		shardBlock.MerklePatriciaRoot,
		shardBlock.Height,
		shardBlock.CommitmentProof,
		shardBlock.ShardState,
		shardBlock.ValMapping,
		shardBlock.NofMiners,
	}
	return SerializeHashContent(blockHash)
}

func (shardBlock *ShardBlock) Encode() []byte {
	if shardBlock == nil {
		return nil
	}

	encoded := ShardBlock{
		Header:                shardBlock.Header,
		Hash:                  shardBlock.Hash,
		PrevBlockHash:         shardBlock.PrevBlockHash,
		Timestamp:             shardBlock.Timestamp,
		MerkleRoot:            shardBlock.MerkleRoot,
		MerklePatriciaRoot:    shardBlock.MerklePatriciaRoot,
		Height:                shardBlock.Height,
		CommitmentProof:	   shardBlock.CommitmentProof,
		ShardState:			   shardBlock.ShardState,
		ValMapping:			   shardBlock.ValMapping,
		NofMiners:			   shardBlock.NofMiners,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (shardBlock *ShardBlock) EncodeHeader() []byte {
	if shardBlock == nil {
		return nil
	}

	encoded := ShardBlock{
		Header:          shardBlock.Header,
		Hash:            shardBlock.Hash,
		PrevBlockHash: 	 shardBlock.PrevBlockHash,
		Height:          shardBlock.Height,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (shardBlock *ShardBlock) Decode(encoded []byte) (b *ShardBlock) {
	if encoded == nil {
		return nil
	}

	var decoded ShardBlock
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (shardBlock ShardBlock) String() string {
	return fmt.Sprintf("Hash: %x\n"+
		"Prev Block Hash: %v\n"+
		"Timestamp: %v\n"+
		"MerkleRoot: %x\n"+
		"MerklePatriciaRoot: %x\n"+
		"Height: %d\n"+
		"Commitment Proof: %x\n" +
		"Shard State: \n%v\n" +
		"Validator Shard Mapping: %s\n" +
		"Number of Miners: %d\n",
		shardBlock.Hash[0:8],
		shardBlock.PrevBlockHash,
		shardBlock.Timestamp,
		shardBlock.MerkleRoot[0:8],
		shardBlock.MerklePatriciaRoot,
		shardBlock.Height,
		shardBlock.CommitmentProof[0:8],
		shardBlock.StringShardState(),
		shardBlock.ValMapping.String(),
		shardBlock.NofMiners,
	)
}

func (shardBlock ShardBlock) StringShardState() (state string) {
	for _, acc := range shardBlock.ShardState {
		state += fmt.Sprintf("Is root: %v\n", acc)
	}
	return state
}
