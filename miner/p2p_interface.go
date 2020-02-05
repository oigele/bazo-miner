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


func incomingTransactionAssignment() {
	for {
		transactionAssignment := <- p2p.TransactionAssignmentIn
		processAssignmentData(transactionAssignment)
	}
}

func incomingCommitteeCheck() {
	for {
		committeeCheck := <- p2p.CommitteeCheckIn
		processCommitteeCheck(committeeCheck)
	}
}

func processEpochBlock(eb []byte) {
	var epochBlock *protocol.EpochBlock
	epochBlock = epochBlock.Decode(eb)

	if(storage.ReadClosedEpochBlock(epochBlock.Hash) != nil){
		logger.Printf("Received Epoch Block (%x) already in storage\n", epochBlock.Hash[0:8])
		p2p.EpochBlockReceivedChan <- *epochBlock
		return
	} else {
		if !storage.IsCommittee {
			//only take the epoch block if it's actually the following epoch block. If not, dont take it yet. It will be rebroadcasted later anyways
			if lastEpochBlock == nil || epochBlock.Height == lastBlock.Height + 1 {
				logger.Printf("Received Epoch Block: %v\n", epochBlock.String())
				storage.WriteClosedEpochBlock(epochBlock)

				storage.DeleteAllLastClosedEpochBlock()
				storage.WriteLastClosedEpochBlock(epochBlock)

				lastEpochBlock = epochBlock

				p2p.EpochBlockReceivedChan <- *lastEpochBlock

				broadcastEpochBlock(lastEpochBlock)
			}
		} else {
			//dont immediately take all attributes from the epoch block to local memory
			logger.Printf("Received Epoch Block: %v\n", epochBlock.String())
			lastEpochBlock = epochBlock
			storage.WriteClosedEpochBlock(epochBlock)
			storage.DeleteAllLastClosedEpochBlock()
			storage.WriteLastClosedEpochBlock(epochBlock)
			broadcastEpochBlock(lastEpochBlock)
		}
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
			} else {
				logger.Printf("Received state transition already included: Shard ID: %v  VS my shard ID: %v - Height: %d - Hash: %x\n",stateTransition.ShardID,storage.ThisShardID,stateTransition.Height,stateHash[0:8])
			}
	}
}

//No check if committee because only committee process this message in the first place.
func processCommitteeCheck(payload []byte) {
	var committeeCheck *protocol.CommitteeCheck
	committeeCheck = committeeCheck.DecodeCommitteeCheck(payload)
	if lastEpochBlock != nil {
		checkHash := committeeCheck.HashCommitteCheck()
		if storage.ReceivedCommitteeCheckStash.CommitteeCheckIncluded(checkHash) == false {
			logger.Printf("Writing committee check to stash. Height: %d, Sender: %x", committeeCheck.Height, committeeCheck.Sender[0:8])
			storage.ReceivedCommitteeCheckStash.Set(checkHash, committeeCheck)
		} else {
			logger.Printf("Received committee check already included")
		}
	}
}


func processAssignmentData(payload []byte) {
	var transactionAssignment *protocol.TransactionAssignment
	transactionAssignment = transactionAssignment.DecodeTransactionAssignment(payload)
	//safety check and only store the transaction assignment of the own shard
	if !storage.IsCommittee {
		if lastEpochBlock != nil && transactionAssignment.ShardID == storage.ThisShardID {
			//got the desired transaction assignment. write it to the channel which will be consumed after epoch block reception
			logger.Printf("received the transaction assignment from a broadcast. writing to request channel")
			p2p.TransactionAssignmentReqChan <- payload
		}
		//is committee. Store all assignments
	} else {
		if lastEpochBlock != nil {
			if !storage.ReceivedTransactionAssignmentStash.TransactionAssignmentIncluded(transactionAssignment.HashTransactionAssignment()) {
				logger.Printf("Writing transaction assignment to stash. Height: %d, ShardID: %d", transactionAssignment.Height, transactionAssignment.ShardID)
				storage.ReceivedTransactionAssignmentStash.Set(transactionAssignment.HashTransactionAssignment(), transactionAssignment)
			} else {
				logger.Printf("Received transaction assignment already included")
			}
		}
	}
}


func processBlock(payload []byte) {
	var block *protocol.Block
	block = block.Decode(payload)
	blockHash := block.HashBlock()


	if storage.IsCommittee {
		if (lastEpochBlock != nil) {
			logger.Printf("Received block (%x) from shard %d with height: %d\n", block.Hash[0:8], block.ShardId, block.Height)
			if storage.ReceivedShardBlockStash.BlockIncluded(blockHash) == false {
				logger.Printf("Writing block to stash Shard ID: %v  - Height: %d - Hash: %x\n", block.ShardId, block.Height, blockHash[0:8])
				storage.ReceivedShardBlockStash.Set(blockHash, block)
			}
		} else {
			logger.Printf("Received block (%x) already in block stash\n", block.Hash[0:8])
		}
	}
}


func broadcastEpochBlock(epochBlock *protocol.EpochBlock) {
	logger.Printf("broadcasting an epoch block")
	p2p.EpochBlockOut <- epochBlock.Encode()
}

func broadcastStateTransition(st *protocol.StateTransition) {
	p2p.StateTransitionOut <- st.EncodeTransition()
}

//here Kürsat's code ends

func broadcastAssignmentData(data *protocol.TransactionAssignment) {
	p2p.TransactionAssignmentOut <- data.EncodeTransactionAssignment()
}

func broadcastCommitteeCheck(cc *protocol.CommitteeCheck) {
	p2p.CommitteeCheckOut <- cc.EncodeCommitteeCheck()
}

//p2p.BlockOut is a channel whose data get consumed by the p2p package
func broadcastBlock(block *protocol.Block) {
	p2p.BlockOut <- block.Encode()

	//Make a deep copy of the block (since it is a pointer and will be saved to db later).
	//Otherwise the block's bloom filter is initialized on the original block.
	var blockCopy = *block
	//blockCopy.InitBloomFilter(append(storage.GetTxPubKeys(&blockCopy)))
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

func broadcastVerifiedAggDataTxsToOtherMiners(txs []*protocol.AggDataTx) {
	for _, tx := range txs {
		toBrdcst := p2p.BuildPacket(p2p.AGGDATATX_BRDCST, tx.Encode())
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
	}
}

func broadcastVerifiedFundsTxsToOtherMiners(txs []*protocol.FundsTx) {

	for _, tx := range txs {
		toBrdcst := p2p.BuildPacket(p2p.FUNDSTX_BRDCST, tx.Encode())
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
	}
}
