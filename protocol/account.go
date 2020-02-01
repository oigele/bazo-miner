package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
)

type Account struct {
	Address            [64]byte              // 64 Byte
	Issuer             [32]byte              // 32 Byte
	Balance            uint64                // 8 Byte
	TxCnt              uint32                // 4 Byte
	IsStaking          bool                  // 1 Byte
	IsCommittee		   bool					 // 1 Byte
	CommitmentKey      [crypto.COMM_KEY_LENGTH]byte // represents the modulus N of the RSA public key
	CommitteeKey	   [crypto.COMM_KEY_LENGTH]byte // represents the modulus N of the RSA public key
	StakingBlockHeight uint32                // 4 Byte
	Contract           []byte                // Arbitrary length
	ContractVariables  []ByteArray           // Arbitrary length
}

func NewAccount(address [64]byte,
	issuer [32]byte,
	balance uint64,
	isStaking bool,
	isCommittee bool,
	commitmentKey [crypto.COMM_KEY_LENGTH]byte,
	committeeKey [crypto.COMM_KEY_LENGTH]byte,
	contract []byte,
	contractVariables []ByteArray) Account {

	newAcc := Account{
		address,
		issuer,
		balance,
		0,
		isStaking,
		isCommittee,
		commitmentKey,
		committeeKey,
		0,
		contract,
		contractVariables,
	}

	return newAcc
}

func (acc *Account) Hash() [32]byte {
	if acc == nil {
		return [32]byte{}
	}

	return SerializeHashContent(acc.Address)
}

func (acc *Account) Encode() []byte {
	if acc == nil {
		return nil
	}

	encoded := Account{
		Address:            acc.Address,
		Issuer:             acc.Issuer,
		Balance:            acc.Balance,
		TxCnt:              acc.TxCnt,
		IsStaking:          acc.IsStaking,
		IsCommittee:		acc.IsCommittee,
		CommitmentKey:   	acc.CommitmentKey,
		CommitteeKey:       acc.CommitteeKey,
		StakingBlockHeight: acc.StakingBlockHeight,
		Contract:           acc.Contract,
		ContractVariables:  acc.ContractVariables,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*Account) Decode(encoded []byte) (acc *Account) {
	var decoded Account
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (acc Account) String() string {
	addressHash := acc.Hash()
	return fmt.Sprintf(
		"Hash: %x, " +			//TODO: uncomment this
			"Address: %x, " +
			//"Issuer: %x, " +
			"TxCnt: %v, " +
			"Balance: %v, " +
			"IsStaking: %v, " +
			"IsCommittee: %v, " +
			//+
			"CommitmentKey: %x, "+
			"CommitteeKey: %x",
			//"StakingBlockHeight: %v, " +
			//"Contract: %v, " +
			//"ContractVariables: %v",
		addressHash[0:8],
		acc.Address[0:8],
		//acc.Issuer[0:8],
		acc.TxCnt,
		acc.Balance,
		acc.IsStaking,
		acc.IsCommittee,

		acc.CommitmentKey[0:8],
		acc.CommitteeKey[0:8],
		//acc.CommitmentKey[0:8],
		//acc.StakingBlockHeight,
		//acc.Contract,
		//acc.ContractVariables)
	)
}
