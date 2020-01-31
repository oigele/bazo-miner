package protocol

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"time"
)

const (
	FUNDSTX_SIZE = 246
)

//when we broadcast transactions we need a way to distinguish with a type

type FundsTx struct {
	Header 		byte
	Amount 		uint64
	Fee    		uint64
	TxCnt  		uint32
	TimeStamp	int64
	From   		[32]byte
	To     		[32]byte
	Sig1   		[64]byte
	Sig2   		[64]byte
	//says if the fundsTx has been aggregated into an aggtx. This boolean isn't needed in the current version
	Aggregated 	bool
	Block		[32]byte //This saves the blockHashWithoutTransactions into which the transaction was usually validated. Needed for rollback.
	Data   		[]byte
}

func ConstrFundsTx(header byte, amount uint64, fee uint64, txCnt uint32, from, to [32]byte, sig1Key *ecdsa.PrivateKey, sig2Key *ecdsa.PrivateKey, data []byte) (tx *FundsTx, err error) {
	tx = new(FundsTx)

	tx.Header = header
	tx.From = from
	tx.To = to
	tx.Amount = amount
	tx.Fee = fee
	tx.TxCnt = txCnt
	tx.Aggregated = false
	tx.Data = data
	tx.Block = [32]byte{}
	tx.TimeStamp = time.Now().UnixNano()

	txHash := tx.Hash()

	r, s, err := ecdsa.Sign(rand.Reader, sig1Key, txHash[:])
	if err != nil {
		return nil, err
	}

	copy(tx.Sig1[32-len(r.Bytes()):32], r.Bytes())
	copy(tx.Sig1[64-len(s.Bytes()):], s.Bytes())

	if sig2Key != nil {
		r, s, err := ecdsa.Sign(rand.Reader, sig2Key, txHash[:])
		if err != nil {
			return nil, err
		}

		copy(tx.Sig2[32-len(r.Bytes()):32], r.Bytes())
		copy(tx.Sig2[64-len(s.Bytes()):], s.Bytes())
	}

	return tx, nil
}

func (tx *FundsTx) Copy() (newTx *FundsTx) {
	newTx = new(FundsTx)
	newTx.Header = tx.Header
	newTx.From = tx.From
	newTx.To = tx.To
	newTx.Amount = tx.Amount
	newTx.Fee = tx.Fee
	newTx.TxCnt = tx.TxCnt
	newTx.Aggregated = false
	newTx.Data = tx.Data
	newTx.Block = [32]byte{}
	newTx.Sig1 = tx.Sig1
	newTx.Sig2 = tx.Sig2

	return newTx
}

func (tx *FundsTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Header byte
		Amount uint64
		Fee    uint64
		TxCnt  uint32
		TimeStamp	int64
		From   [32]byte
		To     [32]byte
		Data   []byte
	}{
		tx.Header,
		tx.Amount,
		tx.Fee,
		tx.TxCnt,
		tx.TimeStamp,
		tx.From,
		tx.To,
		tx.Data,
	}

	return SerializeHashContent(txHash)
}

//when we serialize the struct with binary.Write, unexported field get serialized as well, undesired
//behavior. Therefore, writing own encoder/decoder
func (tx *FundsTx) Encode() (encodedTx []byte) {
	// Encode
	encodeData := FundsTx{
		Header: 	tx.Header,
		Amount: 	tx.Amount,
		Fee:    	tx.Fee,
		TxCnt:  	tx.TxCnt,
		TimeStamp:	tx.TimeStamp,
		From:   	tx.From,
		To:     	tx.To,
		Sig1:   	tx.Sig1,
		Sig2:   	tx.Sig2,
		Data:   	tx.Data,
		Aggregated: tx.Aggregated,
		Block: 		tx.Block,
	}
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encodeData)
	return buffer.Bytes()
}

func (*FundsTx) Decode(encodedTx []byte) *FundsTx {
	var decoded FundsTx
	buffer := bytes.NewBuffer(encodedTx)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *FundsTx) TxFee() uint64 { return tx.Fee }
func (tx *FundsTx) Size() uint64  { return FUNDSTX_SIZE }

func (tx *FundsTx) Sender() [32]byte { return tx.From }
func (tx *FundsTx) Receiver() [32]byte { return tx.To }

func (tx FundsTx) String() string {
	return fmt.Sprintf(
		"\nHeader: %v\n"+
			"Amount: %v\n"+
			"Fee: %v\n"+
			"TxCnt: %v\n"+
			"Timestamp: %v\n" +
			"From: %x\n"+
			"To: %x\n"+
			"Sig1: %x\n"+
			"Sig2: %x\n"+
			"Data: %v\n"+
			"Aggregated: %t\n"+
			"Block: %x",
		tx.Header,
		tx.Amount,
		tx.Fee,
		tx.TxCnt,
		tx.TimeStamp,
		tx.From[0:8],
		tx.To[0:8],
		tx.Sig1[0:8],
		tx.Sig2[0:8],
		tx.Data,
		tx.Aggregated,
		tx.Block,
	)
}
