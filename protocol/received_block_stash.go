package protocol

import (
	"sync"
)
/*This datastructe maintains a map of the form [32]byte - *Block. It stores the blcoks received from the shards.
This datastructure will be queried after every epoch block to check if we can continue to the next epoch.
Because we need to remove the first element of this datastructure and map access is random in Go, we additionally have a slice datastructure
which keeps track of the order of the included state transition. Such that, using the slice structure, we can remove the first received block once this
stash gets full*/
type KeyBlock [32]byte   // Key: Hash of the block
type ValueBlock *Block // Value: Block

type BlockStash struct {
	M    map[KeyBlock]ValueBlock
	Keys []KeyBlock
}

var blockMutex			= &sync.Mutex{}

func NewShardBlockStash() *BlockStash {
	return &BlockStash{M: make(map[KeyBlock]ValueBlock)}
}



/*This function includes a key and tracks its order in the slice*/
func (m *BlockStash) Set(k KeyBlock, v ValueBlock) {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	/*Check if the map does not contain the key*/
	if _, ok := m.M[k]; !ok {
		m.Keys = append(m.Keys, k)
		m.M[k] = v
	}

	/*When length of stash is > 50 --> Remove first added Block*/
	if(len(m.M) > 50){
		m.DeleteFirstEntry()
	}
}

func (m *BlockStash) BlockIncluded(k KeyBlock) bool {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	/*Check if the map does not contain the key*/
	if _, ok := m.M[k]; !ok {
		return false
	} else {
		return true
	}
}

/*This function includes a key and tracks its order in the slice. No need to put the lock because it is used from the calling function*/
func (m *BlockStash) DeleteFirstEntry() {
	firstBlockHash := m.Keys[0]

	if _, ok := m.M[firstBlockHash]; ok {
		delete(m.M,firstBlockHash)
	}
	m.Keys = append(m.Keys[:0], m.Keys[1:]...)
}

/*This function counts how many blocks in the stash have some predefined height*/
func CheckForHeightBlock(blockStash *BlockStash, height uint32) int {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	numberOfBlocksAtHeight := 0
	for _,block := range blockStash.M {
		if block.Height == height {
			numberOfBlocksAtHeight++
		}
	}
	return numberOfBlocksAtHeight
}

func ReturnBlockStashForHeight(blockStash *BlockStash, height uint32) [] *Block {
	blockMutex.Lock()
	defer blockMutex.Unlock()

	blockSlice := []*Block{}

	for _,b := range blockStash.M {
		if b.Height == height {
			blockSlice = append(blockSlice,b)
		}
	}

	return blockSlice
}

func ReturnBlockHashesForHeight(blockStash *BlockStash, height uint32) [][32]byte {
	blockMutex.Lock()
	defer blockMutex.Unlock()

	hashSlice := [][32]byte{}

	for _,b := range blockStash.M {
		if b.Height == height {
			hashSlice = append(hashSlice,b.Hash)
		}
	}

	return hashSlice
}

func ReturnBlockForPosition(blockStash *BlockStash, position int) (stateHash [32]byte, block *Block) {
	blockMutex.Lock()
	defer blockMutex.Unlock()

	if(position > len(blockStash.Keys)-1){
		return [32]byte{}, nil
	}

	stateStashPos := blockStash.Keys[position]

	return stateStashPos, blockStash.M[stateStashPos]
}
