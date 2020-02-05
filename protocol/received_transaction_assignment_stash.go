package protocol

import (
	"sync"
)
/*This datastructe maintains a map of the form int - *TransactionAssignment. It stores the committee checks received from other committee members.
This datastructure will be queried at every blockheight to check if every one played according to the rules.
Because we need to remove the first element of this datastructure and map access is random in Go, we additionally have a slice datastructure
which keeps track of the order of the included transaction assignment. Such that, using the slice structure, we can remove the first received block once this
stash gets full*/
type KeyAssignment [32]byte  // Key: Hash of the committee check
type ValueAssignment *TransactionAssignment // Value: Transaction Assignment

type TransactionAssignmentStash struct {
	M    map[KeyAssignment]ValueAssignment
	Keys []KeyAssignment
}

var transactionAssignmentMutex			= &sync.Mutex{}

func NewTransactionAssignmentStash() *TransactionAssignmentStash {
	return &TransactionAssignmentStash{M: make(map[KeyAssignment]ValueAssignment)}
}



/*This function includes a key and tracks its order in the slice*/
func (m *TransactionAssignmentStash) Set(k KeyAssignment, v ValueAssignment) {
	transactionAssignmentMutex.Lock()
	defer transactionAssignmentMutex.Unlock()
	/*Check if the map does not contain the key*/
	if _, ok := m.M[k]; !ok {
		m.Keys = append(m.Keys, k)
		m.M[k] = v
	}

	/*When lenght of stash is > 50 --> Remove first added Block*/
	if(len(m.M) > 50){
		m.DeleteFirstEntry()
	}
}

func (m *TransactionAssignmentStash) TransactionAssignmentIncluded(k KeyAssignment) bool {
	transactionAssignmentMutex.Lock()
	defer transactionAssignmentMutex.Unlock()
	/*Check if the map does not contain the key*/
	if _, ok := m.M[k]; !ok {
		return false
	} else {
		return true
	}
}

/*This function includes a key and tracks its order in the slice. No need to put the lock because it is used from the calling function*/
func (m *TransactionAssignmentStash) DeleteFirstEntry() {
	firstStateTransitionHash := m.Keys[0]

	if _, ok := m.M[firstStateTransitionHash]; ok {
		delete(m.M,firstStateTransitionHash)
	}
	m.Keys = append(m.Keys[:0], m.Keys[1:]...)
}

/*This function counts how many transaction assignments in the stash have some predefined height*/
func CheckForHeightTransactionAssignment(transactionAssignmentStash *TransactionAssignmentStash, height uint32) int {
	transactionAssignmentMutex.Lock()
	defer transactionAssignmentMutex.Unlock()
	numberOfTransactionAssignmentForHeight := 0
	for _,transactionAssignment := range transactionAssignmentStash.M {
		if transactionAssignment.Height == int(height) {
			numberOfTransactionAssignmentForHeight += 1
		}
	}
	return numberOfTransactionAssignmentForHeight
}

func ReturnTransactionAssignmentForHeight(transactionAssignmentStash *TransactionAssignmentStash, height uint32) [] *TransactionAssignment {
	transactionAssignmentMutex.Lock()
	defer transactionAssignmentMutex.Unlock()

	transactionAssignmentSlice := []*TransactionAssignment{}

	for _,ta := range transactionAssignmentStash.M {
		if(ta.Height == int(height)){
			transactionAssignmentSlice = append(transactionAssignmentSlice,ta)
		}
	}

	return transactionAssignmentSlice
}

/*
func ReturnShardHashesForHeight(statestash *StateStash, height uint32) [][32]byte {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	hashSlice := [][32]byte{}

	for _,st := range statestash.M {
		if(st.Height == int(height)){
			hashSlice = append(hashSlice,st.BlockHash)
		}
	}

	return hashSlice
}
 */


///*This function returns the hashes of the blocks for some height*/
//func ReturnHashesForHeight(blockstash *BlockStash, height uint32) (hashes [][32]byte) {
//	stashMutex.Lock()
//	defer stashMutex.Unlock()
//	var blockHashes [][32]byte
//
//	for _,block := range blockstash.M {
//		if(block.Height == height){
//			blockHashes = append(blockHashes,block.Hash)
//		}
//	}
//	return blockHashes
//}
//
///*This function extracts the transaction hashes of the blocks for some height*/
//func ReturnTxPayloadForHeight(blockstash *BlockStash, height uint32) (txpayload []*TransactionPayload) {
//	stashMutex.Lock()
//	defer stashMutex.Unlock()
//	payloadSlice := []*TransactionPayload{}
//
//	for _,block := range blockstash.M {
//		if(block.Height == height){
//			payload := NewTransactionPayload(block.ShardId,int(block.Height),nil,nil,nil,nil)
//			payload.StakeTxData = block.StakeTxData
//			payload.ConfigTxData = block.ConfigTxData
//			payload.FundsTxData = block.FundsTxData
//			payload.ContractTxData = block.ContractTxData
//			payloadSlice = append(payloadSlice,payload)
//		}
//	}
//	return payloadSlice
//}
//
///*This function extracts the item at some position*/
//func ReturnItemForPosition(blockstash *BlockStash, position int) (blockHash [32]byte, block *Block) {
//	stashMutex.Lock()
//	defer stashMutex.Unlock()
//
//	if(position > len(blockstash.Keys)-1){
//		return [32]byte{}, nil
//	}
//
//	blockHashPos := blockstash.Keys[position]
//
//	return blockHashPos, blockstash.M[blockHashPos]
//}