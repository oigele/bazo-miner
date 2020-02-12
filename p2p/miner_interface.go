package p2p

import (
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"sync"
)

var (
	//Block from the network, to the miner
	BlockIn = make(chan []byte, 1000)
	//Block from the miner, to the network
	BlockOut = make(chan []byte, 100)
	//BlockHeader from the miner, to the clients
	BlockHeaderOut = make(chan []byte)

	//State transition from the miner to the network
	StateTransitionOut = make(chan []byte)

	//Transaction assignment from the committee to the network
	TransactionAssignmentOut = make(chan []byte)

	//Committee check from committee to the network
	CommitteeCheckOut = make(chan []byte)

	//State transition from the network to the miner
	StateTransitionIn = make(chan []byte)

	//Transaction Assignment from the network to the miner
	TransactionAssignmentIn = make(chan []byte)

	//CommitteeCheck from the network to the committee
	CommitteeCheckIn = make(chan []byte)

	//EpochBlock from the network, to the miner
	EpochBlockIn = make(chan []byte)
	//EpochBlock from the miner, to the network
	EpochBlockOut = make(chan []byte)

	EpochBlockReceivedChan = make(chan protocol.EpochBlock)

	VerifiedTxsOut       = make(chan []byte)
	VerifiedTxsBrdcstOut = make(chan []byte, 1000)

	//Data requested by miner, to allow parallelism, we have a chan for every tx type.
	FundsTxChan   = make(chan *protocol.FundsTx)
	AccTxChan     = make(chan *protocol.AccTx)
	ConfigTxChan  = make(chan *protocol.ConfigTx)
	StakeTxChan   = make(chan *protocol.StakeTx)
	AggTxChan     = make(chan *protocol.AggTx)
	DataTxChan    = make(chan *protocol.DataTx)
	AggDataTxChan = make(chan *protocol.AggDataTx)

	BlockReqChan                = make(chan []byte)
	StateTransitionShardReqChan = make(chan []byte)
	CommitteeCheckReqChan		= make(chan []byte)
	StateTransitionShardOut     = make(chan []byte)
	CommitteeCheckShardOut 		= make(chan []byte)
	ShardBlockShardOut          = make(chan []byte)

	TransactionAssignmentReqOut = make(chan []byte)

	ShardBlockReqChan            = make(chan []byte)
	TransactionAssignmentReqChan = make(chan []byte)

	FirstEpochBlockReqChan = make(chan []byte)
	EpochBlockReqChan      = make(chan []byte)
	LastEpochBlockReqChan  = make(chan []byte)
	GenesisReqChan         = make(chan []byte)

	ValidatorShardMapReq = make(chan []byte)

	ReceivedFundsTXStash = make([]*protocol.FundsTx, 0)
	ReceivedAggTxStash   = make([]*protocol.AggTx, 0)
	ReceivedStakeTxStash = make([]*protocol.StakeTx, 0)
	ReceivedAccTxStash   = make([]*protocol.AccTx, 0)
	ReceivedDataTxStash  = make([]*protocol.DataTx, 0)
	ReceivedAggDataTxStash = make([]*protocol.AggDataTx, 0)

	fundsTxSashMutex  = &sync.Mutex{}
	aggTxStashMutex   = &sync.Mutex{}
	aggDataTxStashMutex = &sync.Mutex{}
	blockStashMutex   = &sync.Mutex{}
	stakeTxStashMutex = &sync.Mutex{}
	accTxStashMutex   = &sync.Mutex{}
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
		blockHeader := <-BlockHeaderOut
		clientBrdcstMsg <- BuildPacket(BLOCK_HEADER_BRDCST, blockHeader)
	}
}

func forwardStateTransitionShardToMiner() {
	for {
		st := <-StateTransitionShardOut
		logger.Printf("Building state transition request packet\n")
		toBrdcst := BuildPacket(STATE_TRANSITION_REQ, st)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardCommitteeCheckRequestToMiner() {
	for {
		cc := <- CommitteeCheckShardOut
		logger.Printf("Building committee check request packet\n")
		toBrdcst := BuildPacket(COMMITTEE_CHECK_REQ, cc)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardShardBlockRequestToMiner() {
	for {
		block := <-ShardBlockShardOut
		logger.Printf("Building shard block request packet\n")
		toBrdcst := BuildPacket(SHARD_BLOCK_REQ, block)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardTransactionAssignmentRequestToMiner() {
	for {
		transactionAssignment := <-TransactionAssignmentReqOut
		logger.Printf("Building transaction assignment request packet \n")
		toBrdcst := BuildPacket(TRANSACTION_ASSIGNMENT_REQ, transactionAssignment)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardStateTransitionBrdcstToMiner() {
	for {
		st := <-StateTransitionOut
		toBrdcst := BuildPacket(STATE_TRANSITION_BRDCST, st)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardTransactionAssignmentBrdcstToMiner() {
	for {
		transactionAssignment := <-TransactionAssignmentOut
		toBrdcst := BuildPacket(TRANSACTION_ASSIGNMENT_BRDCST, transactionAssignment)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardCommitteeCheckToMiner() {
	for {
		committeeCheck := <- CommitteeCheckOut
		toBrdcst := BuildPacket(COMMITTEE_CHECK_BRDCST, committeeCheck)
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardEpochBlockBrdcstToMiner() {
	for {
		epochBlock := <-EpochBlockOut
		toBrdcst := BuildPacket(EPOCH_BLOCK_BRDCST, epochBlock)
		logger.Printf("Build Epoch Block Brdcst Packet...\n")
		minerBrdcstMsg <- toBrdcst
	}
}

func forwardVerifiedTxsToMiner() {
	for {
		verifiedTxs := <-VerifiedTxsOut
		clientBrdcstMsg <- BuildPacket(VERIFIEDTX_BRDCST, verifiedTxs)
	}
}

func forwardVerifiedTxsBrdcstToMiner() {
	for {
		verifiedTx := <-VerifiedTxsBrdcstOut
		minerBrdcstMsg <- verifiedTx
	}
}

func forwardBlockToMiner(p *peer, payload []byte) {
	//	blockStashMutex.Lock()
	//	var block *protocol.Block
	//	block = block.Decode(payload)
	//	storage.WriteToReceivedStash(block)
	//	if !BlockAlreadyReceived(storage.ReadReceivedBlockStash(),block.Hash){
	/*if len(BlockIn) > 0 {
		var block *protocol.Block
		block = block.Decode(payload)
		logger.Printf("Inside ForwardBlockToMiner --> len(BlockIn) = %v for block %x", len(BlockIn), block.Hash[0:8])
	}*/
	BlockIn <- payload
	//	}
	//	blockStashMutex.Unlock()
}

//Checks if Tx Is in the received stash. If true, we received the transaction with a request already.
func FundsTxAlreadyInStash(slice []*protocol.FundsTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func AggTxAlreadyInStash(slice []*protocol.AggTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func DataTxAlreadyInStash(slice []*protocol.DataTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func AggDataTxAlreadyInStash(slice []*protocol.AggDataTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func StakeTxAlreadyInStash(slice []*protocol.StakeTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func AccTxAlreadyInStash(slice []*protocol.AccTx, newTXHash [32]byte) bool {
	for _, txInStash := range slice {
		if txInStash.Hash() == newTXHash {
			return true
		}
	}
	return false
}

func BlockAlreadyReceived(slice []*protocol.Block, newBlockHash [32]byte) bool {
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
		if !FundsTxAlreadyInStash(ReceivedFundsTXStash, fundsTx.Hash()) {
			ReceivedFundsTXStash = append(ReceivedFundsTXStash, fundsTx)
			FundsTxChan <- fundsTx
			if len(ReceivedFundsTXStash) > 100 {
				ReceivedFundsTXStash = append(ReceivedFundsTXStash[:0], ReceivedFundsTXStash[1:]...)
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
		if !AccTxAlreadyInStash(ReceivedAccTxStash, accTx.Hash()) {
			ReceivedAccTxStash = append(ReceivedAccTxStash, accTx)
			AccTxChan <- accTx
			if len(ReceivedAccTxStash) > 100 {
				ReceivedAccTxStash = append(ReceivedAccTxStash[:0], ReceivedAccTxStash[1:]...)
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
		if !StakeTxAlreadyInStash(ReceivedStakeTxStash, stakeTx.Hash()) {
			ReceivedStakeTxStash = append(ReceivedStakeTxStash, stakeTx)
			StakeTxChan <- stakeTx
			if len(ReceivedStakeTxStash) > 100 {
				ReceivedStakeTxStash = append(ReceivedStakeTxStash[:0], ReceivedStakeTxStash[1:]...)
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
		if !AggTxAlreadyInStash(ReceivedAggTxStash, aggTx.Hash()) {
			ReceivedAggTxStash = append(ReceivedAggTxStash, aggTx)
			AggTxChan <- aggTx
			if len(ReceivedAggTxStash) > 100 {
				ReceivedAggTxStash = append(ReceivedAggTxStash[:0], ReceivedAggTxStash[1:]...)
			}
		}
		aggTxStashMutex.Unlock()
	case AGGDATATX_RES:
		var aggDataTx *protocol.AggDataTx
		aggDataTx = aggDataTx.Decode(payload)
		if aggDataTx == nil {
			return
		}
		aggDataTxStashMutex.Lock()
		if !AggDataTxAlreadyInStash(ReceivedAggDataTxStash, aggDataTx.Hash()) {
			ReceivedAggDataTxStash = append(ReceivedAggDataTxStash, aggDataTx)
			AggDataTxChan <- aggDataTx
			if len(ReceivedAggDataTxStash) > 100 {
				ReceivedAggDataTxStash = append(ReceivedAggDataTxStash[:0], ReceivedAggDataTxStash[1:]...)
			}
		}
		aggDataTxStashMutex.Unlock()
	}
}

func forwardBlockReqToMiner(p *peer, payload []byte) {
	var block *protocol.Block
	block = block.Decode(payload)

	blockStashMutex.Lock()
	if !BlockAlreadyReceived(storage.ReadReceivedBlockStash(), block.Hash) {
		storage.WriteToReceivedStash(block)
		BlockReqChan <- payload
	}
	blockStashMutex.Unlock()
}

func forwardEpochBlockToMinerIn(p *peer, payload []byte) {
	logger.Printf("Writing Epoch block to channel EpochBlockIn from %s.\n", p.getIPPort())
	EpochBlockIn <- payload
}

func forwardStateTransitionToMiner(p *peer, payload []byte) () {
	StateTransitionIn <- payload
}

func forwardTransactionAssignmentToMinerIn(p *peer, payload []byte) {
	TransactionAssignmentIn <- payload
}

func forwardCommitteeCheckToMinerIn(p *peer, payload []byte) {
	CommitteeCheckIn <- payload
}

func forwardLastEpochBlockToMiner(p *peer, payload []byte) {
	LastEpochBlockReqChan <- payload
}

func forwardStateTransitionShardReqToMiner(p *peer, payload []byte) {
	logger.Printf("received state transition response..\n")
	StateTransitionShardReqChan <- payload
}

func forwardTransactionAssignmentToMiner(p *peer, payload []byte) {
	logger.Printf("received transaction assignment response...\n")
	TransactionAssignmentReqChan <- payload
}

func forwardCommitteeCheckReqToMiner(p *peer, payload []byte) {
	logger.Printf("received committee check request response... \n")
	CommitteeCheckReqChan <- payload
}

func forwardShardBlockToMiner(p *peer, payload []byte) {
	logger.Printf("received shard block response... \n")
	ShardBlockReqChan <- payload
}

func forwardGenesisReqToMiner(p *peer, payload []byte) {
	GenesisReqChan <- payload
}

func forwardFirstEpochBlockToMiner(p *peer, payload []byte) {
	FirstEpochBlockReqChan <- payload
}

func forwardEpochBlockToMiner(p *peer, payload []byte) {
	EpochBlockReqChan <- payload
}

func ReadSystemTime() int64 {
	return systemTime
}
