package protocol

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"encoding/gob"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
)

const (
	//TODO Recalculate
	COMMITTEETX_SIZE = 0
)

//when we broadcast transactions we need a way to distinguish with a type

type CommitteeTx struct {
	Header        byte                  // 1 Byte
	Fee           uint64                // 8 Byte
	IsCommittee   bool                  // 1 Byte
	Account       [32]byte              // 32 Byte
	Sig           [64]byte              // 64 Byte
	CommitteeKey  [crypto.COMM_KEY_LENGTH]byte // the modulus N of the RSA public key
}

func ConstrCommitteeTx(header byte, fee uint64, isCommittee bool, account [32]byte, signKey *ecdsa.PrivateKey, commPubKey *rsa.PublicKey) (tx *CommitteeTx, err error) {

	tx = new(CommitteeTx)

	tx.Header = header
	tx.Fee = fee
	tx.IsCommittee = isCommittee
	tx.Account = account

	copy(tx.CommitteeKey[:], commPubKey.N.Bytes())

	txHash := tx.Hash()

	r, s, err := ecdsa.Sign(rand.Reader, signKey, txHash[:])
	if err != nil {
		return nil, err
	}

	copy(tx.Sig[32-len(r.Bytes()):32], r.Bytes())
	copy(tx.Sig[64-len(s.Bytes()):], s.Bytes())

	return tx, nil
}

func (tx *CommitteeTx) Hash() (hash [32]byte) {
	if tx == nil {
		//is returning nil better?
		return [32]byte{}
	}

	txHash := struct {
		Header      	 byte
		Fee         	 uint64
		IsCommittee 	 bool
		Account     	 [32]byte
		Sig 			 [64]byte
		CommitteeKey     [crypto.COMM_KEY_LENGTH]byte
	}{
		tx.Header,
		tx.Fee,
		tx.IsCommittee,
		tx.Account,
		tx.Sig,
		tx.CommitteeKey,
	}

	return SerializeHashContent(txHash)
}


func (tx *CommitteeTx) Encode() (encodedTx []byte) {
	if tx == nil {
		return nil
	}

	encoded := CommitteeTx{
		Header:  	  tx.Header,
		Fee:    	  tx.Fee,
		IsCommittee:  tx.IsCommittee,
		Account:	  tx.Account,
		Sig:   		  tx.Sig,
		CommitteeKey: tx.CommitteeKey,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*CommitteeTx) Decode(encoded []byte) (tx *CommitteeTx) {
	var decoded CommitteeTx
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (tx *CommitteeTx) TxFee() uint64 { return tx.Fee }
func (tx *CommitteeTx) Size() uint64  { return COMMITTEETX_SIZE }
func (tx *CommitteeTx) Sender() [32]byte { return [32]byte{} } //return empty because it is not needed.
func (tx *CommitteeTx) Receiver() [32]byte { return [32]byte{}}

func (tx CommitteeTx) String() string {
	return fmt.Sprintf(
		"\nHeader: %x\n"+
			"Fee: %v\n"+
			"IsCommittee: %v\n"+
			"Account: %x\n"+
			"Sig: %x\n"+
			"CommitteeKey: %x\n",
		tx.Header,
		tx.Fee,
		tx.IsCommittee,
		tx.Account[0:8],
		tx.Sig[0:8],
		tx.CommitteeKey[0:8],
	)
}
