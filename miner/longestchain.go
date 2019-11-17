package miner

import (
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"time"
)

//Function to give a list of blocks to rollback (in the right order) and a list of blocks to validate.
//Covers both cases (if block belongs to the longest chain or not).
func getBlockSequences(newBlock *protocol.Block) (blocksToRollback, blocksToValidate []*protocol.Block, err error) {
	//Fetch all blocks that are needed to validate.
	//ancestorhash is the last block that is saved in the storage as already validated
	//newchain is the chain behind the new block and after the previously mentioned ancestor
	ancestorHash, newChain := getNewChain(newBlock)
	//Common ancestor not found, discard block.
	if ancestorHash == [32]byte{} {
		return nil, nil, errors.New("Common ancestor not found.")
	}

	//Count how many blocks there are on the currently active chain.
	tmpBlock := lastBlock

	if tmpBlock == nil {
		tmpBlock = storage.ReadLastClosedBlock()
	}

	if tmpBlock == nil {
		return nil, nil, errors.New("Last Block not found")
	}
	/* Removed fabios Code. He immediately throws an error if he doesnt find a block that he should find. I don't see a valid reason for that so
	I decided to use Kürsats code
	var blocksToRollbackMutex = sync.Mutex{}
	for {
		blocksToRollbackMutex.Lock()
		// TODO add || tmpBlock.HashWithoutTx == ancestor.HashWithoutTx
		if tmpBlock.Hash == ancestorHash {
			break
		}
		blocksToRollback = append(blocksToRollback, tmpBlock)
		//The block needs to be in closed storage.
		newTmpBlock := storage.ReadClosedBlock(tmpBlock.PrevHash)

		//Search in blocks withoutTx.
		//TODO uncomment the next two commented lines
		if newTmpBlock == nil {
//			newTmpBlock = storage.ReadClosedBlockWithoutTx(tmpBlock.PrevHashWithoutTx)
		}
		if newTmpBlock == nil {
//			logger.Printf("Block not found: %x, %x", tmpBlock.Hash, tmpBlock.HashWithoutTx)
			blocksToRollbackMutex.Unlock()
			//TODO find out what is the consequence of that is
			return nil, nil, errors.New(fmt.Sprintf("Block not found in both closed storages"))
		}
		tmpBlock = newTmpBlock
		blocksToRollbackMutex.Unlock()
	}
	 */

	//This for loop is Kürsats solution
	for tmpBlock.Height > lastEpochBlock.Height{
		if tmpBlock.Hash == ancestorHash {
			break
		}
		blocksToRollback = append(blocksToRollback, tmpBlock)
		logger.Printf("Added block (%x) to rollback blocks\n",tmpBlock.Hash[0:8])
		//The block needs to be in closed storage.
		tmpBlockNewHash := tmpBlock.PrevHash
		tmpBlock = storage.ReadClosedBlock(tmpBlockNewHash)
		if(tmpBlock != nil){
			logger.Printf("New tmpBlock: (%x)\n",tmpBlock.Hash[0:8])
		} else {
			logger.Printf("tmpBlock is nil. No Block found in closed storage for hash: (%x)\n",tmpBlockNewHash[0:8])
			if(ancestorHash == storage.ReadLastClosedEpochBlock().Hash){
				break
			}
		}
	}

	//Compare current length with new chain length.
	if len(blocksToRollback) >= len(newChain) {
		//Current chain length is longer or equal (our consensus protocol states that in this case we reject the block).
		return nil, nil, errors.New(fmt.Sprintf("Block belongs to shorter or equally long chain --> NO Rollback (blocks to rollback %d vs block of new chain %d)", len(blocksToRollback), len(newChain)))
	} else {
		//New chain is longer, rollback and validate new chain.
		if len(blocksToRollback) != 0 {

			logger.Printf("Rollback (blocks to rollback %d vs block of new chain %d)", len(blocksToRollback), len(newChain))
			logger.Printf("ANCESTOR: %x", ancestorHash[0:8])

		}
		return blocksToRollback, newChain, nil
	}
}

//Returns the ancestor from which the split occurs (if a split occurred, if not it's just our last block) and a list
//of blocks that belong to a new chain.
//basically returns a list of blocks that are not in the closed storage yet
func getNewChain(newBlock *protocol.Block) (ancestor [32]byte, newChain []*protocol.Block) {
	found := false
	for {
		newChain = append(newChain, newBlock)


		//Search for an ancestor (which needs to be in closed storage -> validated block).
		//Search in closed (Validated) blocks first
		potentialAncestor := storage.ReadClosedBlock(newBlock.PrevHash)
		prevBlockHash := newBlock.PrevHash

		if potentialAncestor != nil {
			//Found ancestor because it is found in our closed block storage.
			//We went back in time, so reverse order.
			newChain = InvertBlockArray(newChain)
			return potentialAncestor.Hash, newChain
		} else {
			//Check if ancestor is an epoch block
			potentialEpochAncestorHash := storage.ReadLastClosedEpochBlock().Hash
			if prevBlockHash == potentialEpochAncestorHash {
				//Found ancestor because it is found in our closed block storage.
				//We went back in time, so reverse order.
				newChain = InvertBlockArray(newChain)
				return potentialEpochAncestorHash, newChain
			}
		}




/*		TODO uncomment block
		potentialAncestor = storage.ReadClosedBlockWithoutTx(newBlock.PrevHashWithoutTx)
		if potentialAncestor != nil {
			//Found ancestor because it is found in our closed block storage.
			//We went back in time, so reverse order.
			newChain = InvertBlockArray(newChain)
			return potentialAncestor, newChain
		}
*/
		//It might be the case that we already started a sync and the block is in the openblock storage.
		openBlock := storage.ReadOpenBlock(newBlock.PrevHash)
		if openBlock != nil {
			continue
		}

		// Check if block is in received stash. When in there, continue outer for-loop, until ancestor
		// is found in closed block storage. The blocks from the stash will be validated in the normal validation process
		// after the rollback. (Similar like when in open storage) If not in stash, continue with a block request to
		// the network. Keep block in stash in case of multiple rollbacks (Very rare)
		for _, block := range storage.ReadReceivedBlockStash() {
			if block.Hash == newBlock.PrevHash {
				newBlock = block
				found = true
				break
			}
		}

		if found {
			found = false
			continue
		}

		//Fetch the block we apparently missed from the network.
		//p2p.BlockReq(newBlock.PrevHash, newBlock.PrevHashWithoutTx)
		requestHash := newBlock.PrevHash
		//Todo change the call back to request both blocks at once
//		requestHashWithoutTx := newBlock.PrevHashWithoutTx
		logger.Printf("Getting previous block. But I think the previous block is an Epoch Block.")
		p2p.BlockReq(requestHash, requestHash)

		//Blocking wait
		select {
		case encodedBlock := <-p2p.BlockReqChan:
			newBlock = newBlock.Decode(encodedBlock)
			storage.WriteToReceivedStash(newBlock)
		//Limit waiting time to BLOCKFETCH_TIMEOUT seconds before aborting.
		case <-time.After(BLOCKFETCH_TIMEOUT * time.Second):
			logger.Printf("Timed Out fetching %x in longestChain -> Search in received Block stash", requestHash)
			if p2p.BlockAlreadyReceived(storage.ReadReceivedBlockStash(), requestHash) {
				for _, block := range storage.ReadReceivedBlockStash() {
					if block.Hash == requestHash {
						newBlock = block
						logger.Printf("Block %x was in Received Block Stash", requestHash)
						break
					}
				}
				break
			}
			return [32]byte{}, nil		}
	}

	return [32]byte{}, nil

}
