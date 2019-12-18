package miner

import (
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
)

//The code in this source file communicates with the p2p package via channels

//Constantly listen to incoming data from the network
func incomingData() {
	for {
		block := <-p2p.BlockIn
		processBlock(block)
	}
}

//Constantly listen to incoming epoch block data from the network
//Code from Kürsat
func incomingEpochData() {
	for {
		//receive Epoch Block
		logger.Printf("Listening to incoming epoch blocks...")
		epochBlock := <-p2p.EpochBlockIn
		logger.Printf("Retrieved Epoch block from channel EpochBlockIn.\n")
		processEpochBlock(epochBlock)
	}
}

//Constantly listen to incoming state transition data from the network
func incomingStateData(){
	for{
		stateTransition := <- p2p.StateTransitionIn
		processStateData(stateTransition)
	}
}

func incomingBlockStateData() {
	for {
		blockTransition := <- p2p.BlockTransitionIn
		processBlockStateData(blockTransition)
	}
}

//Code from Kürsat
func processEpochBlock(eb []byte) {
	var epochBlock *protocol.EpochBlock
	epochBlock = epochBlock.Decode(eb)

	if(storage.ReadClosedEpochBlock(epochBlock.Hash) != nil){
		logger.Printf("Received Epoch Block (%x) already in storage\n", epochBlock.Hash[0:8])
		return
	} else {
		//Accept the last received epoch block as the valid one. From the epoch block, retrieve the global state and the
		//valiadator-shard mapping. Upon successful acceptance, broadcast the epoch block
		logger.Printf("Received Epoch Block: %v\n", epochBlock.String())
		ValidatorShardMap = epochBlock.ValMapping
		NumberOfShards = epochBlock.NofShards
		storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress]
		storage.ThisShardMap[int(epochBlock.Height)] = storage.ThisShardID
		lastEpochBlock = epochBlock
		storage.WriteClosedEpochBlock(epochBlock)

		storage.DeleteAllLastClosedEpochBlock()
		storage.WriteLastClosedEpochBlock(epochBlock)

		p2p.EpochBlockReceivedChan <- *lastEpochBlock

		broadcastEpochBlock(lastEpochBlock)
	}
}

func processStateData(payload []byte) {
	var stateTransition *protocol.StateTransition
	stateTransition = stateTransition.DecodeTransition(payload)
	if(lastEpochBlock != nil){
		//removed the check whether the shard id is the same as the id now. This will never lead to any inconsistencies and makes it easier to handle state transitions which reach over an epoch block.
			stateHash := stateTransition.HashTransition()
			if (storage.ReceivedStateStash.StateTransitionIncluded(stateHash) == false){
				logger.Printf("Writing state to stash Shard ID: %v  VS my shard ID: %v - Height: %d - Hash: %x\n",stateTransition.ShardID,storage.ThisShardID,stateTransition.Height,stateHash[0:8])
				storage.ReceivedStateStash.Set(stateHash,stateTransition)
				logger.Printf("Length state stash map: %d\n",len(storage.ReceivedStateStash.M))
				logger.Printf("Length state stash keys: %d\n",len(storage.ReceivedStateStash.Keys))
				logger.Printf("Redistributing state transition\n")
				broadcastStateTransition(stateTransition)
			//There are a lot of connectivity problems. Oftentimes, only the root node is connected to all other nodes. Therefore, the root node will in all cases broadcast any incoming state transitions.
			} else if p2p.IsBootstrap() {
				logger.Printf("Sharing state transition of shard: %d height: %d, in case it hasnt reached its destination yet", stateTransition.ShardID, stateTransition.Height)
				broadcastStateTransition(stateTransition)
			} else {
				logger.Printf("Received state transition already included: Shard ID: %v  VS my shard ID: %v - Height: %d - Hash: %x\n",stateTransition.ShardID,storage.ThisShardID,stateTransition.Height,stateHash[0:8])
				return
			}
	}
}

//End code from Kürsat

func processBlockStateData(payload []byte) {
	//todo implement
}

//ReceivedBlockStash is a stash with all Blocks received such that we can prevent forking
func processBlock(payload []byte) {

	var block *protocol.Block
	block = block.Decode(payload)

	//What follows is Kürsat's mechanism to deal with any incoming blocks (not Epoch Blocks)
	if(lastEpochBlock != nil){
		logger.Printf("Received block (%x) from shard %d with height: %d\n", block.Hash[0:8],block.ShardId,block.Height)
		//For blocks, generally don't use the delayed shard id.
		if(!storage.BlockAlreadyInStash(storage.ReceivedBlockStash,block.Hash) && block.ShardId != storage.ThisShardID){
			storage.WriteToReceivedStash(block)
			broadcastBlock(block)
		} else {
			logger.Printf("Received block (%x) already in block stash\n",block.Hash[0:8])
		}
		//for blocks, generally use the non-delayed shard id
		if block.ShardId == storage.ThisShardID && block.Height > lastEpochBlock.Height {
			//Block already confirmed and validated
			if storage.ReadClosedBlock(block.Hash) != nil {
				logger.Printf("Received block (%x) has already been validated.\n", block.Hash[0:8])
				return
			}
			//If block belongs to my shard, validate it
			err := validate(block, false)
			if err == nil {
				logger.Printf("Received Validated block: %vState:\n%v\n", block, getState())
			} else {
				logger.Printf("Received block (%x) could not be validated: %v\n", block.Hash[0:8], err)
			}

			if(block.Height == lastEpochBlock.Height +1){
				logger.Printf(`"EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x" -> "Hash : %x \n Height : %d"`+"\n", block.PrevHash[0:8],lastEpochBlock.Height,lastEpochBlock.MerklePatriciaRoot[0:8],block.Hash[0:8],block.Height)
				logger.Printf(`"EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+`[color = red, shape = box]`+"\n",block.PrevHash[0:8],lastEpochBlock.Height,lastEpochBlock.MerklePatriciaRoot[0:8])
			} else {
				logger.Printf(`"Hash : %x \n Height : %d" -> "Hash : %x \n Height : %d"`+"\n", block.PrevHash[0:8],block.Height-1,block.Hash[0:8],block.Height)
			}
		}
	}



/*
	//Block already confirmed and validated
	if storage.ReadClosedBlock(block.Hash) != nil {
		logger.Printf("Received block (%x) has already been validated.\n", block.Hash[0:8])
		return
	}


	//Append received Block to stash
	storage.WriteToReceivedStash(block)


	//Start validation process
	receivedBlockInTheMeantime = true
	err := validate(block, false)
	receivedBlockInTheMeantime = false
	if err == nil {
		go broadcastBlock(block)
		logger.Printf("Validated block (received): %vState:\n%v", block, getState())
	} else {
		logger.Printf("Received block (%x) could not be validated: %v\n", block.Hash[0:8], err)
	}*/
}


func broadcastEpochBlock(epochBlock *protocol.EpochBlock) {
	logger.Printf("Writing Epoch block (%x) to channel EpochBlockOut\n", epochBlock.Hash[0:8])
	p2p.EpochBlockOut <- epochBlock.Encode()
}

func broadcastStateTransition(st *protocol.StateTransition) {
	p2p.StateTransitionOut <- st.EncodeTransition()
}

//here Kürsat's code ends

//p2p.BlockOut is a channel whose data get consumed by the p2p package
func broadcastBlock(block *protocol.Block) {
	p2p.BlockOut <- block.Encode()

	//Make a deep copy of the block (since it is a pointer and will be saved to db later).
	//Otherwise the block's bloom filter is initialized on the original block.
	var blockCopy = *block
	blockCopy.InitBloomFilter(append(storage.GetTxPubKeys(&blockCopy)))
	p2p.BlockHeaderOut <- blockCopy.EncodeHeader()
}

func broadcastVerifiedFundsTxs(txs []*protocol.FundsTx) {
	var verifiedTxs [][]byte

	for _, tx := range txs {
		verifiedTxs = append(verifiedTxs, tx.Encode()[:])
	}

	p2p.VerifiedTxsOut <- protocol.Encode(verifiedTxs, protocol.FUNDSTX_SIZE)
}

func broadcastVerifiedAggTxsToOtherMiners(txs []*protocol.AggTx) {
	for _, tx := range txs {
		toBrdcst := p2p.BuildPacket(p2p.AGGTX_BRDCST, tx.Encode())
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
	}
}

func broadcastVerifiedFundsTxsToOtherMiners(txs []*protocol.FundsTx) {

	for _, tx := range txs {
		toBrdcst := p2p.BuildPacket(p2p.FUNDSTX_BRDCST, tx.Encode())
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
	}
}
