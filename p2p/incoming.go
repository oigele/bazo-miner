package p2p

//All incoming messages are processed here and acted upon accordingly
func processIncomingMsg(p *peer, header *Header, payload []byte) {

	//logger.Printf("Received Message Type: %s from IP Port: %s", LogMapping[header.TypeID], p.getIPPort())

	switch header.TypeID {
	//BROADCASTING
	case FUNDSTX_BRDCST:
		processTxBrdcst(p, payload, FUNDSTX_BRDCST)
	case ACCTX_BRDCST:
		processTxBrdcst(p, payload, ACCTX_BRDCST)
	case CONFIGTX_BRDCST:
		processTxBrdcst(p, payload, CONFIGTX_BRDCST)
	case STAKETX_BRDCST:
		processTxBrdcst(p, payload, STAKETX_BRDCST)
	case AGGTX_BRDCST:
		processTxBrdcst(p, payload, AGGTX_BRDCST)
	case DATATX_BRDCST:
		processTxBrdcst(p, payload, DATATX_BRDCST)
	case AGGDATATX_BRDCST:
		processTxBrdcst(p, payload, AGGDATATX_BRDCST)
	case COMMITTEETX_BRDCST:
		processTxBrdcst(p, payload, COMMITTEETX_BRDCST)
	case BLOCK_BRDCST:
		forwardBlockToMiner(p, payload)
	case TIME_BRDCST:
		processTimeRes(p, payload)
	case EPOCH_BLOCK_BRDCST:
		forwardEpochBlockToMinerIn(p, payload)
	case STATE_TRANSITION_BRDCST:
		forwardStateTransitionToMiner(p,payload)
	case TRANSACTION_ASSIGNMENT_BRDCST:
		forwardTransactionAssignmentToMinerIn(p, payload)

		//REQUESTS
	case FUNDSTX_REQ:
		txRes(p, payload, FUNDSTX_REQ)
	case ACCTX_REQ:
		txRes(p, payload, ACCTX_REQ)
	case CONFIGTX_REQ:
		txRes(p, payload, CONFIGTX_REQ)
	case STAKETX_REQ:
		txRes(p, payload, STAKETX_REQ)
	case AGGTX_REQ:
		txRes(p, payload, AGGTX_REQ)
	case AGGDATATX_REQ:
		txRes(p, payload, AGGDATATX_REQ)
	case UNKNOWNTX_REQ:
		txRes(p, payload, UNKNOWNTX_REQ)
	case SPECIALTX_REQ:
		specialTxRes(p, payload, SPECIALTX_REQ)
	case NOT_FOUND_TX_REQ:
		if !peerSelfConn(p.getIPPort()) {
			notFoundTxRes(payload)
		}
	case BLOCK_REQ:
		blockRes(p, payload)
	case SHARD_BLOCK_REQ:
		shardBlockRes(p, payload)
	case BLOCK_HEADER_REQ:
		blockHeaderRes(p, payload)
	case ACC_REQ:
		accRes(p, payload)
	case ROOTACC_REQ:
		rootAccRes(p, payload)
	case MINER_PING:
		pongRes(p, payload, MINER_PING)
	case CLIENT_PING:
		pongRes(p, payload, CLIENT_PING)
	case NEIGHBOR_REQ:
		neighborRes(p)
	case INTERMEDIATE_NODES_REQ:
		intermediateNodesRes(p, payload)
	case STATE_TRANSITION_REQ:
		stateTransitionRes(p,payload)
	case GENESIS_REQ:
		genesisRes(p, payload)
	case FIRST_EPOCH_BLOCK_REQ:
		FirstEpochBlockRes(p,payload)
	case EPOCH_BLOCK_REQ:
		EpochBlockRes(p,payload)
	case LAST_EPOCH_BLOCK_REQ:
		LastEpochBlockRes(p,payload)
	case TRANSACTION_ASSIGNMENT_REQ:
		TransactionAssignmentRes(p, payload)

		//RESPONSES
	case NEIGHBOR_RES:
		if !peerSelfConn(p.getIPPort()){
			processNeighborRes(p, payload)
		}
	case VALIDATOR_SHARD_RES:
		processValMappingRes(p, payload)
	case BLOCK_RES:
		forwardBlockReqToMiner(p, payload)
	case FUNDSTX_RES:
		forwardTxReqToMiner(p, payload, FUNDSTX_RES)
	case ACCTX_RES:
		forwardTxReqToMiner(p, payload, ACCTX_RES)
	case STATE_TRANSITION_RES:
		forwardStateTransitionShardReqToMiner(p,payload)
	case CONFIGTX_RES:
		forwardTxReqToMiner(p, payload, CONFIGTX_RES)
	case STAKETX_RES:
		forwardTxReqToMiner(p, payload, STAKETX_RES)
	case AGGTX_RES:
		forwardTxReqToMiner(p, payload, AGGTX_RES)
	case AGGDATATX_RES:
		forwardTxReqToMiner(p, payload, AGGDATATX_RES)
	case GENESIS_RES:
		forwardGenesisReqToMiner(p, payload)
	case FIRST_EPOCH_BLOCK_RES:
		forwardFirstEpochBlockToMiner(p,payload)
	case EPOCH_BLOCK_RES:
		forwardEpochBlockToMiner(p,payload)
	case LAST_EPOCH_BLOCK_RES:
		forwardLastEpochBlockToMiner(p,payload)
	case TRANSACTION_ASSIGNMENT_RES:
		forwardTransactionAssignmentToMiner(p,payload)
	case SHARD_BLOCK_RES:
		forwardShardBlockToMiner(p, payload)
	}


}
