package protocol

import (
	"bytes"
	"encoding/gob"
	"github.com/oigele/bazo-miner/crypto"
	"os"
)


type CommitteeCheck struct {
	Height						int
	Sender						[32]byte
	CommitteeProof				[crypto.COMM_PROOF_LENGTH]byte
	SlashedAddressesCommittee	[][32]byte
	SlashedAddressesShards		[][32]byte
}




func NewCommitteeCheck(height int, sender [32]byte, committeeProof [crypto.COMM_PROOF_LENGTH]byte, slashedAddressesCommittee [][32]byte, slashedAddressesShards [][32]byte) *CommitteeCheck {
	newCommitteeCheck :=  CommitteeCheck{
		height,
		sender,
		committeeProof,
		slashedAddressesCommittee,
		slashedAddressesShards,
	}

	return &newCommitteeCheck
}



func (cc *CommitteeCheck) HashCommitteCheck() [32]byte {
	if cc == nil {
		return [32]byte{}
	}

	ccHash := struct {
		Height				  			  int
		Sender							  [32]byte
	}{
		cc.Height,
		cc.Sender,
	}
	return SerializeHashContent(ccHash)
}


func (cc *CommitteeCheck) EncodeCommitteeCheck() []byte {
	if cc == nil {
		os.Exit(0)
		return nil
	}

	encoded := CommitteeCheck{
		Height:						cc.Height,
		Sender:						cc.Sender,
		CommitteeProof: 			cc.CommitteeProof,
		SlashedAddressesCommittee:	cc.SlashedAddressesCommittee,
		SlashedAddressesShards: 	cc.SlashedAddressesShards,

	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*CommitteeCheck) DecodeCommitteeCheck(encoded []byte) (ta *CommitteeCheck) {
	var decoded CommitteeCheck
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}


