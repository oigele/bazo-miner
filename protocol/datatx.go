package protocol

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/gob"
	"fmt"
)

const (
	//TODO calculate
	DATATX_SIZE = 0
)

//when we broadcast transactions we need a way to distinguish with a type

type DataTx struct {
	Header 		byte
	Fee    		uint64
	TxCnt  		uint32
	From   		[32]byte
	To     		[32]byte
	Sig1   		[64]byte
	Sig2   		[64]byte
	Data   		[]byte
}

func ConstrDataTx(header byte, fee uint64, txCnt uint32, from, to [32]byte, sig1Key *ecdsa.PrivateKey, sig2Key *ecdsa.PrivateKey, data []byte) (tx *DataTx, err error) {
	tx = new(DataTx)

	tx.Header = header
	tx.From = from
	tx.To = to
	tx.Fee = fee
	tx.TxCnt = txCnt
	tx.Data = data

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


func (tx *DataTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Header byte
		Fee    uint64
		TxCnt  uint32
		From   [32]byte
		To     [32]byte
		Data   []byte
	}{
		tx.Header,
		tx.Fee,
		tx.TxCnt,
		tx.From,
		tx.To,
		tx.Data,
	}

	return SerializeHashContent(txHash)
}

//when we serialize the struct with binary.Write, unexported field get serialized as well, undesired
//behavior. Therefore, writing own encoder/decoder
func (tx *DataTx) Encode() (encodedTx []byte) {
	// Encode
	encodeData := DataTx{
		Header: 	tx.Header,
		Fee:    	tx.Fee,
		TxCnt:  	tx.TxCnt,
		From:   	tx.From,
		To:     	tx.To,
		Sig1:   	tx.Sig1,
		Sig2:   	tx.Sig2,
		Data:   	tx.Data,
	}
	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encodeData)
	return buffer.Bytes()
}

func (*DataTx) Decode(encodedTx []byte) *DataTx {
	var decoded DataTx
	buffer := bytes.NewBuffer(encodedTx)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *DataTx) TxFee() uint64 { return tx.Fee }
func (tx *DataTx) Size() uint64  { return FUNDSTX_SIZE }

func (tx *DataTx) Sender() [32]byte { return tx.From }
func (tx *DataTx) Receiver() [32]byte { return tx.To }

func (tx DataTx) String() string {
	return fmt.Sprintf(
		"\nHeader: %v\n"+
			"Fee: %v\n"+
			"TxCnt: %v\n"+
			"From: %x\n"+
			"To: %x\n"+
			"Sig1: %x\n"+
			"Sig2: %x\n"+
			"Data: %v\n",
		tx.Header,
		tx.Fee,
		tx.TxCnt,
		tx.From[0:8],
		tx.To[0:8],
		tx.Sig1[0:8],
		tx.Sig2[0:8],
		tx.Data,
	)
}
