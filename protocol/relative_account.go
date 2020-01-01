package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
)

/**
	The State Transition is the main data type needed in the inter-shard synchronisation mechnism. It contains all data needed to update the local
	state with the state of the other shards and free the Mempool from already validated transactions.
 */
type StateTransition struct {
	RelativeStateChange			map[[32]byte]*RelativeAccount //changed to 32 Byte for streamlining
	Height						int
	ShardID						int
	BlockHash					[32]byte
	AccTxData					[][32]byte
	ContractTxData  			[][32]byte
	FundsTxData  				[][32]byte
	ConfigTxData 				[][32]byte
	StakeTxData  				[][32]byte
	AggTxData					[][32]byte
}

/**
The Block Transition is the main data type needed in the intra-shard synchronisation mechnism. It contains all data needed to update the local
state with the state of the other nodes in a specific shard and free the Mempool from already validated transactions.
*/
type BlockTransition struct {
	RelativeStateChange			map[[32]byte]*RelativeAccount //changed to 32 Byte for streamlining
	Height						int
	ShardID						int
	BlockID						int
	BlockHash					[32]byte
	AccTxData					[][32]byte
	ContractTxData  			[][32]byte
	FundsTxData  				[][32]byte
	ConfigTxData 				[][32]byte
	StakeTxData  				[][32]byte
	AggTxData					[][32]byte
}

/**
	This datastructure keeps track of relative change in the account information during the creation of a block
 */
type RelativeAccount struct {
	Address            [64]byte              // 64 Byte
	Issuer             [32]byte              // 32 Byte Changed to streamline
	Balance            int64                // 8 Byte
	TxCnt              int32                // 4 Byte
	IsStaking          bool                  // 1 Byte
	CommitmentKey      [crypto.COMM_KEY_LENGTH]byte // represents the modulus N of the RSA public key
	StakingBlockHeight int32                // 4 Byte
	Contract           []byte                // Arbitrary length
	ContractVariables  []ByteArray           // Arbitrary length
}

func NewStateTransition(stateChange map[[32]byte]*RelativeAccount, height int, shardid int, blockHash [32]byte,  accTxData [][32]byte, contractData [][32]byte,
	fundsData [][32]byte, configData [][32]byte, stakeData [][32]byte, aggTxData [][32]byte) *StateTransition {
	newTransition := StateTransition{
		stateChange,
		height,
		shardid,
		blockHash,
		accTxData,
		contractData,
		fundsData,
		configData,
		stakeData,
		aggTxData,
	}

	return &newTransition
}

func NewBlockTransition(stateChange map[[32]byte]*RelativeAccount, height int, shardid int, blockid int, blockHash [32]byte, accData [][32]byte, contractData [][32]byte,
	fundsData [][32]byte, configData [][32]byte, stakeData [][32]byte, aggTxData [][32]byte) *BlockTransition {
	newTransition := BlockTransition{
		stateChange,
		height,
		shardid,
		blockid,
		blockHash,
		accData,
		contractData,
		fundsData,
		configData,
		stakeData,
		aggTxData,
	}
	return &newTransition
	}

func NewRelativeAccount(address [64]byte,
	issuer [32]byte,
	balance int64,
	isStaking bool,
	commitmentKey [crypto.COMM_KEY_LENGTH]byte,
	contract []byte,
	contractVariables []ByteArray) RelativeAccount {

	newAcc := RelativeAccount{
		address,
		issuer,
		balance,
		0,
		isStaking,
		commitmentKey,
		0,
		contract,
		contractVariables,
	}

	return newAcc
}

func (st *StateTransition) HashTransition() [32]byte {
	if st == nil {
		return [32]byte{}
	}

	stHash := struct {
		Height				  			  int
		ShardID							  int
	}{
		st.Height,
		st.ShardID,
	}
	return SerializeHashContent(stHash)
}

func (bt *BlockTransition) HashTransition() [32]byte {
	if bt == nil {
		return [32]byte{}
	}

	btHash := struct {
		Height				  			  int
		ShardID							  int
		BlockID							  int
	}{
		bt.Height,
		bt.ShardID,
		bt.BlockID,
	}
	return SerializeHashContent(btHash)
}


func (acc *RelativeAccount) Hash() [32]byte {
	if acc == nil {
		return [32]byte{}
	}

	return SerializeHashContent(acc.Address)
}

func (st *StateTransition) EncodeStateTransition() []byte {
	if st == nil {
		return nil
	}

	encoded := StateTransition{
		RelativeStateChange:		st.RelativeStateChange,
		Height:						st.Height,
		ShardID:					st.ShardID,
		BlockHash:					st.BlockHash,
		AccTxData:					st.AccTxData,
		ContractTxData:				st.ContractTxData,
		FundsTxData:				st.FundsTxData,
		ConfigTxData:				st.ConfigTxData,
		StakeTxData:				st.StakeTxData,
		AggTxData:					st.AggTxData,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (bt *BlockTransition) EncodeBlockTransition() []byte {
	if bt == nil {
		return nil
	}

	encoded := BlockTransition{
		RelativeStateChange:		bt.RelativeStateChange,
		Height:						bt.Height,
		ShardID:					bt.ShardID,
		BlockID:					bt.BlockID,
		BlockHash:					bt.BlockHash,
		AccTxData: 					bt.AccTxData,
		ContractTxData:				bt.ContractTxData,
		FundsTxData:				bt.FundsTxData,
		ConfigTxData:				bt.ConfigTxData,
		StakeTxData:				bt.StakeTxData,
		AggTxData:					bt.AggTxData,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}



func (*StateTransition) DecodeStateTransition(encoded []byte) (st *StateTransition) {
	var decoded StateTransition
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (*BlockTransition) DecodeBlockTransition(encoded []byte) (st *BlockTransition) {
	var decoded BlockTransition
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (acc *RelativeAccount) Encode() []byte {
	if acc == nil {
		return nil
	}

	encoded := RelativeAccount{
		Address:            acc.Address,
		Issuer:             acc.Issuer,
		Balance:            acc.Balance,
		TxCnt:              acc.TxCnt,
		IsStaking:          acc.IsStaking,
		CommitmentKey:   	acc.CommitmentKey,
		StakingBlockHeight: acc.StakingBlockHeight,
		Contract:           acc.Contract,
		ContractVariables:  acc.ContractVariables,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*RelativeAccount) Decode(encoded []byte) (acc *RelativeAccount) {
	var decoded RelativeAccount
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (acc RelativeAccount) String() string {
	addressHash := acc.Hash()
	return fmt.Sprintf(
		"Hash: %x, " +
			"Address: %x, " +
			"Issuer: %x, " +
			"TxCnt: %v, " +
			"Balance: %v, " +
			"IsStaking: %v, " +
			"CommitmentKey: %x, " +
			"StakingBlockHeight: %v, " +
			"Contract: %v, " +
			"ContractVariables: %v",
		addressHash[0:8],
		acc.Address[0:8],
		acc.Issuer[0:8],
		acc.TxCnt,
		acc.Balance,
		acc.IsStaking,
		acc.CommitmentKey[0:8],
		acc.StakingBlockHeight,
		acc.Contract,
		acc.ContractVariables)
}

