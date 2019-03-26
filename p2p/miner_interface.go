package p2p

import (
	"github.com/bazo-blockchain/bazo-miner/protocol"
	"sync"
)

var (
	//Block from the network, to the miner
	BlockIn = make(chan []byte, 100)
	//Block from the miner, to the network
	BlockOut = make(chan []byte, 100)
	//BlockHeader from the miner, to the clients
	BlockHeaderOut = make(chan []byte)

	VerifiedTxsOut = make(chan []byte)
	VerifiedTxsBrdcstOut = make(chan []byte, 100)

	//Data requested by miner, to allow parallelism, we have a chan for every tx type.
	FundsTxChan  		= make(chan *protocol.FundsTx)
	AccTxChan    		= make(chan *protocol.AccTx)
	ConfigTxChan 		= make(chan *protocol.ConfigTx)
	StakeTxChan  		= make(chan *protocol.StakeTx)
	AggTxChan    		= make(chan *protocol.AggTx)

	BlockReqChan = make(chan []byte)

	receivedTXStash = make([]*protocol.FundsTx, 0)
	receivedAggTxStash = make([]*protocol.AggTx, 0)
	receivedStakeTxStash = make([]*protocol.StakeTx, 0)
	receivedAccTxStash = make([]*protocol.AccTx, 0)
	receivedBlockStash = make([]*protocol.Block, 0)

	fundsTxSashMutex = &sync.Mutex{}
	aggTxStashMutex = &sync.Mutex{}
	blockStashMutex = &sync.Mutex{}
	stakeTxStashMutex = &sync.Mutex{}
	accTxStashMutex = &sync.Mutex{}
)

//This is for blocks and txs that the miner successfully validated.
func forwardBlockBrdcstToMiner() {
	for {
		block := <-BlockOut
		toBrdcst := BuildPacket(BLOCK_BRDCST, block)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardBlockHeaderBrdcstToMiner() {
	for {
		blockHeader := <- BlockHeaderOut
		clientBrdcstMsg <- BuildPacket(BLOCK_HEADER_BRDCST, blockHeader)
	}
}

func forwardVerifiedTxsToMiner() {
	for {
		verifiedTxs := <- VerifiedTxsOut
		clientBrdcstMsg <- BuildPacket(VERIFIEDTX_BRDCST, verifiedTxs)
	}
}

func forwardVerifiedTxsBrdcstToMiner() {
	for {
		verifiedTx := <- VerifiedTxsBrdcstOut
		minerBrdcstMsg <- verifiedTx
	}
}

func forwardBlockToMiner(p *peer, payload []byte) {

	//FABIO
	if len(BlockIn) > 0 {
		var block *protocol.Block
		block = block.Decode(payload)
		logger.Printf("Inside ForwardBlockToMiner --> len(BlockIn) = %v for block %x", len(BlockIn), block.Hash[0:8])
	}
	BlockIn <- payload
}

//Checks if Tx Is in the received stash. If true, we received the transaction with a request already.
func txAlreadyInStash(slice []*protocol.FundsTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func aggTxAlreadyInStash(slice []*protocol.AggTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func stakeTxAlreadyInStash(slice []*protocol.StakeTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func accTxAlreadyInStash(slice []*protocol.AccTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func blockAlreadyReceived(slice []*protocol.Block, newBlockHash [32]byte) bool {
	for _, block := range slice {
		if block.Hash == newBlockHash {
			return true
		}
	}
	return false
}


//These are transactions the miner specifically requested.
func forwardTxReqToMiner(p *peer, payload []byte, txType uint8) {
	if payload == nil {
		return
	}

	switch txType {
	case FUNDSTX_RES:
		var fundsTx *protocol.FundsTx
	fundsTx = fundsTx.Decode(payload)
	if fundsTx == nil {
		return
		}
	// If TX is not received with the last 1000 Transaction, send it through the channel to the TX_FETCH.
	// Otherwise send nothing. This means, that the TX was sent before and we ensure, that only one TX per Broadcast
	// request is going through to the FETCH Request. This should prevent the "Received txHash did not correspond to
	// our request." error
	// The Mutex Lock is needed, because sometimes the execution is too fast. And even with the stash transactions
	// are sent multiple times through the channel.
		// The same concept is used for the AggTx below.
		fundsTxSashMutex.Lock()
		if !txAlreadyInStash(receivedTXStash, fundsTx.Hash()) {
			receivedTXStash = append(receivedTXStash, fundsTx)
			FundsTxChan <- fundsTx
			if len(receivedTXStash) > 5000 {
				receivedTXStash = append(receivedTXStash[:0], receivedTXStash[1:]...)
			}
		}
		fundsTxSashMutex.Unlock()
	case ACCTX_RES:
		var accTx *protocol.AccTx
		accTx = accTx.Decode(payload)
		if accTx == nil {
			return
		}
		accTxStashMutex.Lock()
		if !accTxAlreadyInStash(receivedAccTxStash, accTx.Hash()) {
			receivedAccTxStash = append(receivedAccTxStash, accTx)
			AccTxChan <- accTx
			if len(receivedAccTxStash) > 1000 {
				receivedAccTxStash = append(receivedAccTxStash[:0], receivedAccTxStash[1:]...)
			}
		}
		accTxStashMutex.Unlock()
	case CONFIGTX_RES:
		var configTx *protocol.ConfigTx
		configTx = configTx.Decode(payload)
		if configTx == nil {
			return
		}
		ConfigTxChan <- configTx
	case STAKETX_RES:
		var stakeTx *protocol.StakeTx
		stakeTx = stakeTx.Decode(payload)
		if stakeTx == nil {
			return
		}

		stakeTxStashMutex.Lock()
		if !stakeTxAlreadyInStash(receivedStakeTxStash, stakeTx.Hash()) {
			receivedStakeTxStash = append(receivedStakeTxStash, stakeTx)
			StakeTxChan <- stakeTx
			if len(receivedStakeTxStash) > 1000 {
				receivedStakeTxStash = append(receivedStakeTxStash[:0], receivedStakeTxStash[1:]...)
			}
		}
		stakeTxStashMutex.Unlock()
	case AGGTX_RES:
		var aggTx *protocol.AggTx
		aggTx = aggTx.Decode(payload)
		if aggTx == nil {
			return
		}

		aggTxStashMutex.Lock()
		if !aggTxAlreadyInStash(receivedAggTxStash, aggTx.Hash()) {
			receivedAggTxStash = append(receivedAggTxStash, aggTx)
			AggTxChan <- aggTx
			if len(receivedAggTxStash) > 1000 {
				receivedAggTxStash = append(receivedAggTxStash[:0], receivedAggTxStash[1:]...)
			}
		}
		aggTxStashMutex.Unlock()
	}
}

func forwardBlockReqToMiner(p *peer, payload []byte) {
	var block *protocol.Block
	block = block.Decode(payload)

	blockStashMutex.Lock()
	if !blockAlreadyReceived(receivedBlockStash, block.Hash) {
		receivedBlockStash = append(receivedBlockStash, block)
		BlockReqChan <- payload
		if len(receivedBlockStash) > 10 {
			receivedBlockStash = append(receivedBlockStash[:0], receivedBlockStash[1:]...)
		}
	}
	blockStashMutex.Unlock()
}

func ReadSystemTime() int64 {
	return systemTime
}
