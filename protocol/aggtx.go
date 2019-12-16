package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

const (
	AGGTX_SIZE = 85 //Only constant Values --> Without To, From & AggregatedTxSlice
)

//when we broadcast transactions we need a way to distinguish with a type

type AggTx struct {
	Amount 				uint64
	Fee    				uint64
	From   				[][32]byte
	To    				[][32]byte
	AggregatedTxSlice 	[][32]byte
	Aggregated			bool
	Block				[32]byte //This saves the blockHashWithoutTransactions into which the transaction was usually validated. Needed for rollback.
	MerkleRoot          [32]byte
}

func ConstrAggTx(amount uint64, fee uint64, from [][32]byte, to [][32]byte, transactions [][32]byte) (tx *AggTx, err error) {
	tx = new(AggTx)

	tx.Amount = amount
	tx.Fee = fee
	tx.From = from
	tx.To = to
	tx.AggregatedTxSlice = transactions
	tx.Aggregated = false
	tx.Block = [32]byte{}
	tx.MerkleRoot = BuildAggTxMerkleTree(transactions).MerkleRoot()


	return tx, nil
}


func (tx *AggTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Amount			 	uint64
		Fee    				uint64
		From   				[][32]byte
		To     				[][32]byte
		MerkleRoot 			[32]byte
	}{
		tx.Amount,
		tx.Fee,
		tx.From,
		tx.To,
		tx.MerkleRoot,
	}

	return SerializeHashContent(txHash)
}

//when we serialize the struct with binary.Write, unexported field get serialized as well, undesired
//behavior. Therefore, writing own encoder/decoder
func (tx *AggTx) Encode() (encodedTx []byte) {
	// Encode
	encodeData := AggTx{
		Amount: 				tx.Amount,
		Fee:    				tx.Fee,
		From:					tx.From,
		To:    					tx.To,
		AggregatedTxSlice: 		tx.AggregatedTxSlice,
		Aggregated:				tx.Aggregated,
		Block: 					tx.Block,
		MerkleRoot:				tx.MerkleRoot,
	}
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encodeData)
	return buffer.Bytes()
}

func (*AggTx) Decode(encodedTx []byte) *AggTx {
	var decoded AggTx
	buffer := bytes.NewBuffer(encodedTx)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *AggTx) TxFee() uint64 { return tx.Fee }
func (tx *AggTx) Size() uint64  { return AGGTX_SIZE }

func (tx *AggTx) Sender() [32]byte { return [32]byte{} }
func (tx *AggTx) Receiver() [32]byte { return [32]byte{} }


func (tx AggTx) String() string {
	return fmt.Sprintf(
		"\n ________\n| AGGTX: |____________________________________________________________________\n" +
			"|  Hash: %x\n" +
			"|  Amount: %v\n"+
			"|  Fee: %v\n"+
			"|  From: %x\n"+
			"|  To: %x\n"+
			//"|  Transactions: %x\n"+
			"|  Merkle Root: %x\n"+
			"|  #Tx: %v\n"+
			"|  Aggregated: %t\n" +
			"|_________________________________________________________________________________",
		tx.Hash(),
		tx.Amount,
		tx.Fee,
		tx.From,
		tx.To,
		//tx.AggregatedTxSlice,
		tx.MerkleRoot,
		len(tx.AggregatedTxSlice),
		tx.Aggregated,
	)
}

