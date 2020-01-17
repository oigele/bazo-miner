package protocol

import (
	"bytes"
	"encoding/gob"
	"os"
)


type TransactionAssignment struct {
	Height						int
	ShardID						int
	AccTxs 						[]*AccTx
	StakeTxs					[]*StakeTx
	FundsTxs					[]*FundsTx
}




func NewTransactionAssignment(height int, shardid int, accTxs []*AccTx, stakeTxs []*StakeTx, fundsTxs []*FundsTx) *TransactionAssignment {
	newTransition := TransactionAssignment{
		height,
		shardid,
		accTxs,
		stakeTxs,
		fundsTxs,
	}

	return &newTransition
}



func (ta *TransactionAssignment) HashTransactionAssignment() [32]byte {
	if ta == nil {
		return [32]byte{}
	}

	stHash := struct {
		Height				  			  int
		ShardID							  int
	}{
		ta.Height,
		ta.ShardID,
	}
	return SerializeHashContent(stHash)
}


func (ta *TransactionAssignment) EncodeTransactionAssignment() []byte {
	if ta == nil {
		os.Exit(0)
		return nil
	}

	encoded := TransactionAssignment{
		Height:						ta.Height,
		ShardID:					ta.ShardID,
		AccTxs:						ta.AccTxs,
		StakeTxs:					ta.StakeTxs,
		FundsTxs:					ta.FundsTxs,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*TransactionAssignment) DecodeTransactionAssignment(encoded []byte) (ta *TransactionAssignment) {
	var decoded TransactionAssignment
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}


