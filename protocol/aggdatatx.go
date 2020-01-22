package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

const (
	//TODO recalculate
	AGGDATATX_SIZE = 0 //Only constant Values --> Without To, From & AggregatedTxSlice
)

//when we broadcast transactions we need a way to distinguish with a type

type AggDataTx struct {
	Fee    				uint64
	//only one sender for each aggregated data transaction
	From   				[32]byte
	To    				[][32]byte
	AggregatedDataTx 	[][32]byte
	Data 				[][]byte
}

func ConstrAggDataTx(data [][]byte, fee uint64, from [32]byte, to [][32]byte, transactions [][32]byte) (tx *AggDataTx, err error) {
	tx = new(AggDataTx)

	tx.Data = data
	tx.Fee = fee
	tx.From = from
	tx.To = to
	tx.AggregatedDataTx = transactions

	return tx, nil
}


func (tx *AggDataTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Fee    				uint64
		From   				[32]byte
		To     				[][32]byte
		Data			 	[][]byte
	}{
		tx.Fee,
		tx.From,
		tx.To,
		tx.Data,
	}

	return SerializeHashContent(txHash)
}

//when we serialize the struct with binary.Write, unexported field get serialized as well, undesired
//behavior. Therefore, writing own encoder/decoder
func (tx *AggDataTx) Encode() (encodedTx []byte) {
	// Encode
	encodeData := AggDataTx{
		Fee:    				tx.Fee,
		From:					tx.From,
		To:    					tx.To,
		AggregatedDataTx: 		tx.AggregatedDataTx,
		Data:					tx.Data,
	}
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encodeData)
	return buffer.Bytes()
}

func (*AggDataTx) Decode(encodedTx []byte) *AggDataTx {
	var decoded AggDataTx
	buffer := bytes.NewBuffer(encodedTx)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *AggDataTx) TxFee() uint64 { return tx.Fee }
func (tx *AggDataTx) Size() uint64  { return AGGTX_SIZE }

func (tx *AggDataTx) Sender() [32]byte { return [32]byte{} }
func (tx *AggDataTx) Receiver() [32]byte { return [32]byte{} }


func (tx AggDataTx) String() string {
	return fmt.Sprintf(
		"\n ________\n| AGGDATATX: |____________________________________________________________________\n" +
			"|  Hash: %x\n" +
			"|  Data: %v\n"+
			"|  Fee: %v\n"+
			"|  From: %x\n"+
			"|  To: %x\n"+
			//"|  Transactions: %x\n"+
			"|  #Tx: %v\n"+
			"|_________________________________________________________________________________",
		tx.Hash(),
		tx.Data,
		tx.Fee,
		tx.From,
		tx.To,
		len(tx.AggregatedDataTx),
	)
}

