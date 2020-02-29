package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
	"github.com/willf/bloom"
	"reflect"
)

const (
	HASH_LEN                = 32
	HEIGHT_LEN				= 4
	//All fixed sizes form the Block struct are 254
	MIN_BLOCKSIZE           = 393 + crypto.COMM_PROOF_LENGTH + 1 // = 650 bytes
	MIN_BLOCKHEADER_SIZE    = 104
	BLOOM_FILTER_ERROR_RATE = 0.1
)

type Block struct {
	//Header
	Header      	 	byte
	ShardId		 		int
	Hash         		[32]byte
	PrevHash     		[32]byte
//	HashWithoutTx   	[32]byte 			//valid hash once all tx are aggregated
//	PrevHashWithoutTx  	[32]byte			//valid hash of ancestor once all tx are aggregated
	NrConfigTx   		uint8
	NrElementsBF 		uint16
	BloomFilter  		*bloom.BloomFilter	//8 byte
	Height       		uint32
	Beneficiary  		[32]byte
//	Aggregated			bool 				//Indicates if All transactions are aggregated with a boolean.
	// ==> 177 bytes


	//Body
	Nonce                 [8]byte
	Timestamp             int64
	MerkleRoot            [32]byte
	NrAccTx               uint16
	NrFundsTx             uint16
	NrStakeTx             uint16
	NrCommitteeTx		  uint16
	NrAggTx         	  uint16
	NrDataTx			  uint16
	NrAggDataTx			  uint16
	NrFineTx			  uint16
	SlashedAddress        [32]byte
	CommitmentProof       [crypto.COMM_PROOF_LENGTH]byte
	ConflictingBlockHash1 [32]byte
	ConflictingBlockHash2 [32]byte
//	ConflictingBlockHashWithoutTx1 [32]byte
//	ConflictingBlockHashWithoutTx2 [32]byte
	StateCopy             map[[32]byte]*Account //won't be serialized, just keeping track of local state changes
	// ==> 216 bytes + crypto.COMM_PROOF_LENGTH

	ContractTxData  	 [][32]byte
	AccTxData    		 [][32]byte
	FundsTxData  		 [][32]byte
	DataTxData 			 [][32]byte
	ConfigTxData 		 [][32]byte
	StakeTxData  		 [][32]byte
	CommitteeTxData		 [][32]byte
	AggTxData  	 		 [][32]byte
	AggDataTxData		 [][32]byte
	FineTxData			 [][32]byte
}

func NewBlock(prevHash [32]byte, height uint32) *Block {
	newBlock := Block{
		PrevHash:   prevHash,
		Height:     height,
	}

	newBlock.StateCopy = make(map[[32]byte]*Account)

	return &newBlock
}

func (block *Block) HashBlock() [32]byte {
	if block == nil {
		return [32]byte{}
	}

	blockHash := struct {
		prevHash              			[32]byte
//		prevHashWithoutTx     			[32]byte
		timestamp             			int64
		merkleRoot            			[32]byte
		beneficiary           			[32]byte
		commitmentProof       			[crypto.COMM_PROOF_LENGTH]byte
		slashedAddress        			[32]byte
		conflictingBlockHash1 			[32]byte
		conflictingBlockHash2 			[32]byte
//		conflictingBlockHashWithoutTx1 	[32]byte
//		conflictingBlockHashWithoutTx2 	[32]byte
//		Aggregated			  			bool
	}{
		block.PrevHash,
//		block.PrevHashWithoutTx,
		block.Timestamp,
		block.MerkleRoot,
		block.Beneficiary,
		block.CommitmentProof,
		block.SlashedAddress,
		block.ConflictingBlockHash1,
		block.ConflictingBlockHash2,
//		block.ConflictingBlockHashWithoutTx1,
//		block.ConflictingBlockHashWithoutTx2,
//		false,
	}
	return SerializeHashContent(blockHash)
}

func (block *Block) HashBlockWithoutMerkleRoot() [32]byte {
	if block == nil {
		return [32]byte{}
	}

	blockHash := struct {
		prevHash              			[32]byte
//		prevHashWithoutTx	  			[32]byte
		timestamp             			int64
		merkleRoot            			[32]byte
		beneficiary           			[32]byte
		commitmentProof       			[crypto.COMM_PROOF_LENGTH]byte
		slashedAddress        			[32]byte
		conflictingBlockHash1 			[32]byte
		conflictingBlockHash2 			[32]byte
//		conflictingBlockHashWithoutTx1 	[32]byte
//		conflictingBlockHashWithoutTx2 	[32]byte
//		Aggregated			 			bool
	}{
		block.PrevHash,
//		block.PrevHashWithoutTx,
		block.Timestamp,
		[32]byte{},
		block.Beneficiary,
		block.CommitmentProof,
		block.SlashedAddress,
		block.ConflictingBlockHash1,
		block.ConflictingBlockHash2,
//		block.ConflictingBlockHashWithoutTx1,
//		block.ConflictingBlockHashWithoutTx2,
//		true,
	}
	return SerializeHashContent(blockHash)
}

func (block *Block) InitBloomFilter(txPubKeys [][32]byte) {
	block.NrElementsBF = uint16(len(txPubKeys))

	m, k := calculateBloomFilterParams(float64(len(txPubKeys)), BLOOM_FILTER_ERROR_RATE)
	filter := bloom.New(m, k)
	for _, txPubKey := range txPubKeys {
		filter.Add(txPubKey[:])
	}

	block.BloomFilter = filter
}

func (block *Block) GetSize() uint64 {
	size := MIN_BLOCKSIZE + int(block.GetTxDataSize())

	if block.BloomFilter != nil {
		encodedBF, _ := block.BloomFilter.GobEncode()
		size += len(encodedBF)
	}

	return uint64(size)
}

func (block *Block) GetHeaderSize() uint64 {
	size := int(reflect.TypeOf(block.Header).Size() +
		reflect.TypeOf(block.Hash).Size() +
		reflect.TypeOf(block.PrevHash).Size() +
//		reflect.TypeOf(block.HashWithoutTx).Size() +
//		reflect.TypeOf(block.PrevHashWithoutTx).Size() +
		reflect.TypeOf(block.NrConfigTx).Size() +
		reflect.TypeOf(block.NrElementsBF).Size() +
		reflect.TypeOf(block.Height).Size() +
		reflect.TypeOf(block.Beneficiary).Size())
//		reflect.TypeOf(block.Aggregated).Size())

	size += int(block.GetBloomFilterSize())

	return uint64(size)
}

func (block *Block) GetBodySize() uint64 {
	size := int(reflect.TypeOf(block.Nonce).Size() +
		reflect.TypeOf(block.Timestamp).Size() +
		reflect.TypeOf(block.MerkleRoot).Size() +
		reflect.TypeOf(block.NrAccTx).Size() +
		reflect.TypeOf(block.NrFundsTx).Size() +
		reflect.TypeOf(block.NrStakeTx).Size() +
		reflect.TypeOf(block.NrAggTx).Size() +
		reflect.TypeOf(block.NrDataTx).Size() +
		reflect.TypeOf(block.NrCommitteeTx).Size() +
		reflect.TypeOf(block.NrAggDataTx).Size() +
		reflect.TypeOf(block.NrFineTx).Size() +
		reflect.TypeOf(block.SlashedAddress).Size() +
		reflect.TypeOf(block.CommitmentProof).Size() +
		reflect.TypeOf(block.ConflictingBlockHash1).Size() +
		reflect.TypeOf(block.ConflictingBlockHash2).Size()) +
//		reflect.TypeOf(block.ConflictingBlockHashWithoutTx1).Size() +
//		reflect.TypeOf(block.ConflictingBlockHashWithoutTx2).Size()) +
		int(block.GetTxDataSize())

	size += int(block.GetBloomFilterSize())

	return uint64(size)
}

func (block *Block) GetTxDataSize() uint64 {
	size := int(block.NrAccTx)*HASH_LEN +
		int(block.NrFundsTx)*HASH_LEN +
		int(block.NrConfigTx)*HASH_LEN +
		int(block.NrStakeTx)*HASH_LEN +
		int(block.NrAggTx)*HASH_LEN +
		int(block.NrDataTx)*HASH_LEN +
		int(block.NrAggDataTx)*HASH_LEN+
		int(block.NrCommitteeTx)*HASH_LEN+
		int(block.NrFineTx)*HASH_LEN

	return uint64(size)
}

func (block *Block) GetBloomFilterSize() uint64 {
	size := 0
	if block.BloomFilter != nil {
		encodedBF, _ := block.BloomFilter.GobEncode()
		size += len(encodedBF)
	}

	return uint64(size)
}

func (block *Block) Encode() []byte {
	if block == nil {
		return nil
	}

	encoded := Block{
		Header:                			block.Header,
		ShardId:						block.ShardId,
		Hash:                  			block.Hash,
		PrevHash:              			block.PrevHash,
//		HashWithoutTx:         			block.HashWithoutTx,
//		PrevHashWithoutTx:     			block.PrevHashWithoutTx,
//		Aggregated:			   			block.Aggregated,
		Nonce:                 			block.Nonce,
		Timestamp:             			block.Timestamp,
		MerkleRoot:            			block.MerkleRoot,
		Beneficiary:           			block.Beneficiary,
		NrAccTx:               			block.NrAccTx,
		NrFundsTx:             			block.NrFundsTx,
		NrConfigTx:            			block.NrConfigTx,
		NrStakeTx:             			block.NrStakeTx,
		NrCommitteeTx: 					block.NrCommitteeTx,
		NrAggTx:         				block.NrAggTx,
		NrDataTx:						block.NrDataTx,
		NrAggDataTx: 					block.NrAggDataTx,
		NrFineTx:						block.NrFineTx,
		NrElementsBF:          			block.NrElementsBF,
		BloomFilter:           			block.BloomFilter,
		SlashedAddress:        			block.SlashedAddress,
		Height:                			block.Height,
		CommitmentProof:	   			block.CommitmentProof,
		ConflictingBlockHash1: 			block.ConflictingBlockHash1,
		ConflictingBlockHash2: 			block.ConflictingBlockHash2,
//		ConflictingBlockHashWithoutTx1: block.ConflictingBlockHashWithoutTx1,
//		ConflictingBlockHashWithoutTx2: block.ConflictingBlockHashWithoutTx2,

		AccTxData:    		   			block.AccTxData,
		FundsTxData:  		   			block.FundsTxData,
		ConfigTxData: 		   			block.ConfigTxData,
		StakeTxData:  		   			block.StakeTxData,
		CommitteeTxData:				block.CommitteeTxData,
		AggTxData:	   					block.AggTxData,
		DataTxData:						block.DataTxData,
		AggDataTxData:					block.AggDataTxData,
		FineTxData:						block.FineTxData,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (block *Block) EncodeHeader() []byte {
	if block == nil {
		return nil
	}

	encoded := Block{
		Header:       		block.Header,
		ShardId:			block.ShardId,
		Hash:         		block.Hash,
		PrevHash:     		block.PrevHash,
//		HashWithoutTx:      block.HashWithoutTx,
//		PrevHashWithoutTx:  block.PrevHashWithoutTx,
		NrConfigTx:   		block.NrConfigTx,
		NrElementsBF: 		block.NrElementsBF,
		BloomFilter:  		block.BloomFilter,
		Height:       		block.Height,
		Beneficiary:  		block.Beneficiary,
//		Aggregated:			block.Aggregated,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (block *Block) Decode(encoded []byte) (b *Block) {
	if encoded == nil {
		return nil
	}

	var decoded Block
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (block Block) String() string {
	return fmt.Sprintf("\n" +
		"Hash: %x			"+
		//"Hash Without Tx: %x\n"+
		"Previous Hash: %x		"+
		//"Previous Hash Without Tx: %x\n"+
		//"Aggregated: %t\n"+
		"Nonce: %x\n"+
		"Timestamp: %v\n"+
		"MerkleRoot: %x\n"+
		"Beneficiary: %x\n"+
		"Amount of fundsTx: %v --> %x\n"+
		"Amount of accTx: %v --> %x\n"+
		"Amount of configTx: %v --> %x\n"+
		"Amount of stakeTx: %v --> %x\n"+
		"Amount of committeeTx: %v --> %x\n" +
		"Amount of dataTx: %v --> %x\n" +
		"Amount of aggDataTx: %v --> %x\n" +
		"Amount of aggTx: %v --> %x\n"+
		"Amount of fineTx: %v ---> %x\n" +
		"Total Transactions in this block: %v\n"+
		"Height: %d\n"+
		"Commitment Proof: %x\n"+
		"Slashed Address:%x\n",
//		"Conflicted Block Hashes 1:%x  =  %x\n"+
//		"Conflicted Block Hashes 2:%x  =  %x\n",
		block.Hash[0:8],
		//block.HashWithoutTx[0:8],
		block.PrevHash[0:8],
		//block.PrevHashWithoutTx[0:8],
//		block.Aggregated,
		block.Nonce,
		block.Timestamp,
		block.MerkleRoot[0:8],
		block.Beneficiary[0:8],
		block.NrFundsTx, block.FundsTxData,
		block.NrAccTx, block.AccTxData,
		block.NrConfigTx, block.ConfigTxData,
		block.NrStakeTx, block.StakeTxData,
		block.NrCommitteeTx, block.CommitteeTxData,
		block.NrDataTx, block.DataTxData,
		block.NrAggDataTx, block.AggDataTxData,
		block.NrAggTx, block.AggTxData,
		block.NrFineTx, block.FineTxData,
		uint16(block.NrFundsTx) + uint16(block.NrAccTx) + uint16(block.NrConfigTx) + uint16(block.NrStakeTx) + uint16(block.NrAggTx),
		block.Height,
		block.CommitmentProof[0:8],
		block.SlashedAddress[0:8],
//		block.ConflictingBlockHash1[0:8], block.ConflictingBlockHashWithoutTx1[0:8],
//		block.ConflictingBlockHash2[0:8], block.ConflictingBlockHashWithoutTx2[0:8],
	)
}
