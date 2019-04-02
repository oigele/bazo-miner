package miner

import (
	"github.com/bazo-blockchain/bazo-miner/p2p"
	"github.com/bazo-blockchain/bazo-miner/protocol"
	"github.com/bazo-blockchain/bazo-miner/storage"
	"sync"
)

//The code in this source file communicates with the p2p package via channels

//Constantly listen to incoming data from the network
func incomingData() {
	for {
		block := <-p2p.BlockIn
		if len(p2p.BlockIn) > 0 {
			var block1 *protocol.Block
			block1 = block1.Decode(block)
			logger.Printf("Inside IncommingData --> len(BlockIn) = %v for block %x", len(p2p.BlockIn), block1.Hash[0:8])
		}
		go processBlock(block)
	}
}

var processBlockMutex = &sync.Mutex{}
//ReceivedBlockStash is a stash with all Blocks received such that we can prevent forking
func processBlock(payload []byte) {

	var block *protocol.Block
	block = block.Decode(payload)
	//Block already confirmed and validated
	if storage.ReadClosedBlock(block.Hash) != nil {
		logger.Printf("Received block (%x) has already been validated.\n", block.Hash[0:8])
		return
	}


	//Append received Block to stash
	storage.WriteToReceivedStash(block)


	//Start validation process
	receivedBlockInTheMeantime = true
	//logger.Printf("Inside Validation --> Validation of received Block %x", block.Hash)
	logger.Printf("Inside ProcessBlock ---> Received Block: %x and start Validation in the next step", block.Hash[0:8])
	err := validate(block, false)
	logger.Printf("Inside ProcessBlock ---> End Validation received for block %x, %v", block.Hash[0:8], err)
	receivedBlockInTheMeantime = false
	if err == nil {
		go broadcastBlock(block)
		logger.Printf("Validated block (received): %vState:\n%v", block, getState())
	} else {
		logger.Printf("Received block (%x) could not be validated: %v\n", block.Hash[0:8], err)
	}
}

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
	//logger.Printf("Inside Validation for block --> Inside Postvalidation (11.1) %v", len(txs))
	for _, tx := range txs {
		//logger.Printf("Inside Validation for block --> Inside Postvalidation (11.1.1) %v", len(txs))
		toBrdcst := p2p.BuildPacket(p2p.AGGTX_BRDCST, tx.Encode())
		//logger.Printf("Inside Validation for block --> Inside Postvalidation (11.1.2) %v", len(p2p.VerifiedTxsBrdcstOut))
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
		//logger.Printf("Inside Validation for block --> Inside Postvalidation (11.1.3) %v", len(txs))
	}
}

func broadcastVerifiedFundsTxsToOtherMiners(txs []*protocol.FundsTx) {

	for _, tx := range txs {
		toBrdcst := p2p.BuildPacket(p2p.FUNDSTX_BRDCST, tx.Encode())
		p2p.VerifiedTxsBrdcstOut <- toBrdcst
	}
}
