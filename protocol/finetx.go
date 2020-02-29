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
	FINETX_SIZE = 246 //Todo recalculate
)

//when we broadcast transactions we need a way to distinguish with a type

type FineTx struct {
	Header 		byte
	Amount 		uint64
	Fee    		uint64
	From   		[32]byte
	To     		[32]byte
	Sig   		[64]byte
	TimeStamp	int64
}

func ConstrFineTx(header byte, amount uint64, fee uint64, from [32]byte, to [32]byte, sigKey *ecdsa.PrivateKey) (tx *FineTx, err error) {
	tx = new(FineTx)

	tx.Header = header
	tx.From = from
	tx.To = to
	tx.Amount = amount
	tx.Fee = fee
	tx.TimeStamp = time.Now().UnixNano()
	txHash := tx.Hash()

	r, s, err := ecdsa.Sign(rand.Reader, sigKey, txHash[:])
	if err != nil {
		return nil, err
	}

	copy(tx.Sig[32-len(r.Bytes()):32], r.Bytes())
	copy(tx.Sig[64-len(s.Bytes()):], s.Bytes())

	if sigKey != nil {
		r, s, err := ecdsa.Sign(rand.Reader, sigKey, txHash[:])
		if err != nil {
			return nil, err
		}

		copy(tx.Sig[32-len(r.Bytes()):32], r.Bytes())
		copy(tx.Sig[64-len(s.Bytes()):], s.Bytes())
	}

	return tx, nil
}


func (tx *FineTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Header byte
		Amount uint64
		Fee    uint64
		From   [32]byte
		To     [32]byte
		TimeStamp int64
	}{
		tx.Header,
		tx.Amount,
		tx.Fee,
		tx.From,
		tx.To,
		tx.TimeStamp,
	}

	return SerializeHashContent(txHash)
}

//when we serialize the struct with binary.Write, unexported field get serialized as well, undesired
//behavior. Therefore, writing own encoder/decoder
func (tx *FineTx) Encode() (encodedTx []byte) {
	// Encode
	encodeData := FineTx{
		Header: 	tx.Header,
		Amount: 	tx.Amount,
		Fee:    	tx.Fee,
		From:   	tx.From,
		To:     	tx.To,
		Sig:   		tx.Sig,
		TimeStamp:  tx.TimeStamp,
	}
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encodeData)
	return buffer.Bytes()
}

func (*FineTx) Decode(encodedTx []byte) *FineTx {
	var decoded FineTx
	buffer := bytes.NewBuffer(encodedTx)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *FineTx) TxFee() uint64 { return tx.Fee }
func (tx *FineTx) Size() uint64  { return FUNDSTX_SIZE }

func (tx *FineTx) Sender() [32]byte { return tx.From }
func (tx *FineTx) Receiver() [32]byte { return tx.To }

func (tx FineTx) String() string {
	return fmt.Sprintf(
		"\nHeader: %v\n"+
			"Amount: %v\n"+
			"Fee: %v\n"+
			"From: %x\n"+
			"To: %x\n"+
			"Sig: %x\n",
		tx.Header,
		tx.Amount,
		tx.Fee,
		tx.From[0:8],
		tx.To[0:8],
		tx.Sig[0:8],
	)
}
