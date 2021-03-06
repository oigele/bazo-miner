package protocol

import (
	"bytes"
	"encoding/gob"
	"github.com/oigele/bazo-miner/crypto"
	"os"
)


type TransactionAssignment struct {
	Height						int
	ShardID						int
	CommitteeProof				[crypto.COMM_PROOF_LENGTH]byte
	AccTxs 						[]*AccTx
	StakeTxs					[]*StakeTx
	CommitteeTxs				[]*CommitteeTx
	FundsTxs					[]*FundsTx
	DataTxs						[]*DataTx
	FineTxs						[]*FineTx
}




func NewTransactionAssignment(height int, shardid int, committeeProof [crypto.COMM_PROOF_LENGTH]byte, accTxs []*AccTx, stakeTxs []*StakeTx, committeeTxs []*CommitteeTx, fundsTxs []*FundsTx, dataTxs []*DataTx, fineTxs []*FineTx) *TransactionAssignment {
	newTransition := TransactionAssignment{
		height,
		shardid,
		committeeProof,
		accTxs,
		stakeTxs,
		committeeTxs,
		fundsTxs,
		dataTxs,
		fineTxs,
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
		CommitteeProof					  [crypto.COMM_PROOF_LENGTH]byte
	}{
		ta.Height,
		ta.ShardID,
		ta.CommitteeProof,
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
		CommitteeProof: 			ta.CommitteeProof,
		AccTxs:						ta.AccTxs,
		StakeTxs:					ta.StakeTxs,
		CommitteeTxs:				ta.CommitteeTxs,
		FundsTxs:					ta.FundsTxs,
		DataTxs:					ta.DataTxs,
		FineTxs: 					ta.FineTxs,
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


