package protocol

import (
	"sync"
)
/*This datastructe maintains a map of the form [32]byte - *BlockTransition. It stores the block transitions received from other nodes in the same shard.
This datastructure will be queried at every blockheight to check if we can continue mining the next block.
Because we need to remove the first element of this datastructure and map access is random in Go, we additionally have a slice datastructure
which keeps track of the order of the included state transition. Such that, using the slice structure, we can remove the first received block once this
stash gets full*/
type KeyBlockState [32]byte   // Key: Hash of the block
type ValueBlockState *BlockTransition // Value: Block

type BlockTransitionStash struct {
	M    map[KeyBlockState]ValueBlockState
	Keys []KeyBlockState
}

var stateBlockMutex			= &sync.Mutex{}

func NewBlockTransitionStash() *BlockTransitionStash {
	return &BlockTransitionStash{M: make(map[KeyBlockState]ValueBlockState)}
}

/*This function includes a key and tracks its order in the slice*/
func (m *BlockTransitionStash) Set(k KeyBlockState, v ValueBlockState) {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()
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

func (m *BlockTransitionStash) BlockTransitionIncluded(k KeyBlockState) bool {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()
	/*Check if the map does not contain the key*/
	if _, ok := m.M[k]; !ok {
		return false
	} else {
		return true
	}
}

/*This function includes a key and tracks its order in the slice. No need to put the lock because it is used from the calling function*/
func (m *BlockTransitionStash) DeleteFirstEntry() {
	firstBlockTransitionHash := m.Keys[0]

	if _, ok := m.M[firstBlockTransitionHash]; ok {
		delete(m.M,firstBlockTransitionHash)
	}
	m.Keys = append(m.Keys[:0], m.Keys[1:]...)
}

/*This function counts how many block transisitions in the stash have some predefined height*/
func CheckForHeightBlockTransition(blockTransitionStash *BlockTransitionStash, height uint32) int {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()
	numberOfBlockTransisionsAtHeight := 0
	for _,blockTransision := range blockTransitionStash.M {
		if(blockTransision.Height == int(height)){
			numberOfBlockTransisionsAtHeight = numberOfBlockTransisionsAtHeight + 1
		}
	}
	return numberOfBlockTransisionsAtHeight
}

func ReturnBlockTransitionForHeight(blockTransitionStash *BlockTransitionStash, height uint32) [] *BlockTransition {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()

	var blockTransitionSlice []*BlockTransition

	for _,st := range blockTransitionStash.M {
		if(st.Height == int(height)){
			 blockTransitionSlice = append(blockTransitionSlice,st)
		}
	}

	return blockTransitionSlice
}

func ReturnBlockHashesForHeight(blockTransitionStash *BlockTransitionStash, height uint32) [][32]byte {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()

	hashSlice := [][32]byte{}

	for _,st := range blockTransitionStash.M {
		if(st.Height == int(height)){
			hashSlice = append(hashSlice,st.BlockHash)
		}
	}

	return hashSlice
}

func ReturnBlockTransitionForPosition(blockTransitionStash *BlockTransitionStash, position int) (stateHash [32]byte, blockTransition *BlockTransition) {
	stateBlockMutex.Lock()
	defer stateBlockMutex.Unlock()

	if(position > len(blockTransitionStash.Keys)-1){
		return [32]byte{}, nil
	}

	blockTransitionStashPos := blockTransitionStash.Keys[position]

	return blockTransitionStashPos, blockTransitionStash.M[blockTransitionStashPos]
}
