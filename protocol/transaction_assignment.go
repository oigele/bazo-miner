package protocol

import (
	"bytes"
	"encoding/gob"
)


type TransactionAssignment struct {
	Height						int
	ShardID						int
	Transactions 				[]Transaction
}




func NewTransactionAssignment(height int, shardid int, transactions []Transaction) *TransactionAssignment {
	newTransition := TransactionAssignment{
		height,
		shardid,
		transactions,
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
		return nil
	}

	encoded := TransactionAssignment{
		Height:						ta.Height,
		ShardID:					ta.ShardID,
		Transactions:				ta.Transactions,
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


