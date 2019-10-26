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
	err := validate(block, false)
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
