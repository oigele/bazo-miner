package p2p

import (
	"bytes"
	"encoding/binary"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"strconv"
	"strings"
	"sync"
)

var(
	lastNotFoundTxWithHash = [32]byte{}
	notFoundTxMutex = &sync.Mutex{}
	)

//This file responds to incoming requests from miners in a synchronous fashion
func txRes(p *peer, payload []byte, txKind uint8) {
	var txHash [32]byte
	if len(payload) != 32 {
		return
	}
	copy(txHash[:], payload[0:32])

	var tx protocol.Transaction
	//Check closed and open storage if the tx is available
	openTx := storage.ReadOpenTx(txHash)
	closedTx := storage.ReadClosedTx(txHash)
	invalidTx := storage.ReadINVALIDOpenTx(txHash)

	if openTx != nil {
		tx = openTx
	}
	if closedTx != nil {
		tx = closedTx
	}
	if invalidTx != nil {
		tx = invalidTx
	}

	//In case it was not found, send a corresponding message back
	notFoundTxMutex.Lock()
	if tx == nil {
		packet := BuildPacket(NOT_FOUND, nil)
		sendData(p, packet)
		if lastNotFoundTxWithHash != txHash {
			lastNotFoundTxWithHash = txHash
			//TxReq(txHash, NOT_FOUND_TX_REQ)
		}
		notFoundTxMutex.Unlock()
		return
	}
	notFoundTxMutex.Unlock()

	var packet []byte
	switch txKind {
	case FUNDSTX_REQ:
		packet = BuildPacket(FUNDSTX_RES, tx.Encode())
	case ACCTX_REQ:
		packet = BuildPacket(ACCTX_RES, tx.Encode())
	case CONFIGTX_REQ:
		packet = BuildPacket(CONFIGTX_RES, tx.Encode())
	case STAKETX_REQ:
		packet = BuildPacket(STAKETX_RES, tx.Encode())
	case AGGTX_REQ:
		packet = BuildPacket(AGGTX_RES, tx.Encode())
	case AGGDATATX_REQ:
		packet = BuildPacket(AGGDATATX_RES, tx.Encode())
	case UNKNOWNTX_REQ:
		switch tx.(type) {
		case *protocol.FundsTx:
			packet = BuildPacket(FUNDSTX_RES, tx.Encode())
		case *protocol.AggTx:
			packet = BuildPacket(AGGTX_RES, tx.Encode())
		}
	}
	sendData(p, packet)
}


func notFoundTxRes(payload []byte) {
	var txHash [32]byte
	if len(payload) != 32 {
		return
	}
	copy(txHash[:], payload[0:32])

	var tx protocol.Transaction
	//Check closed and open storage if the tx is available
	openTx := storage.ReadOpenTx(txHash)
	closedTx := storage.ReadClosedTx(txHash)
	invalidTx := storage.ReadINVALIDOpenTx(txHash)

	if openTx != nil {
		tx = openTx
	}
	if closedTx != nil {
		tx = closedTx
	}//
	if invalidTx != nil {
		tx = invalidTx
	}

	//In case it was not found, send a corresponding message back
	notFoundTxMutex.Lock()
	if tx == nil {
		if lastNotFoundTxWithHash != txHash {
			lastNotFoundTxWithHash = txHash
			//TxReq(txHash, NOT_FOUND_TX_REQ)
		}
		notFoundTxMutex.Unlock()
		return
	}
	notFoundTxMutex.Unlock()

	var packet []byte
	switch tx.(type) {
	case *protocol.FundsTx:
		packet = BuildPacket(FUNDSTX_BRDCST, tx.Encode())
	case *protocol.AccTx:
		packet = BuildPacket(ACCTX_BRDCST, tx.Encode())
	case *protocol.ConfigTx:
		packet = BuildPacket(CONFIGTX_BRDCST, tx.Encode())
	case *protocol.StakeTx:
		packet = BuildPacket(STAKETX_BRDCST, tx.Encode())
	case *protocol.AggTx:
		packet = BuildPacket(AGGTX_BRDCST, tx.Encode())
	}

	minerBrdcstMsg <- packet
}

func specialTxRes(p *peer, payload []byte, txKind uint8) {
	//Search Transaction based on the txcnt and sender address.

	var senderHash [32]byte
	var searchedTransaction protocol.Transaction

	if len(payload) != 42 {
		return
	}

	txcnt := binary.BigEndian.Uint32(payload[1:9])
	copy(senderHash[:], payload[10:42])

	for _, txhash := range storage.ReadTxcntToTx(txcnt) {
		tx := storage.ReadOpenTx(txhash)
		if tx != nil {
			if tx.Sender() == senderHash {
				searchedTransaction = tx
				break
			}
		} else {
			tx = storage.ReadINVALIDOpenTx(txhash)
			if tx != nil {
				if tx.Sender() == senderHash {
					searchedTransaction = tx
					break
				}
			} else {
				tx = storage.ReadClosedTx(txhash)
				if tx != nil {
					if tx.Sender() == senderHash {
						searchedTransaction = tx
						break
					}
				}
			}
		}
	}

	if searchedTransaction != nil {
		packet := BuildPacket(FUNDSTX_RES, searchedTransaction.Encode())
		sendData(p, packet)
		packet = BuildPacket(FUNDSTX_BRDCST, searchedTransaction.Encode())
		minerBrdcstMsg <- packet
	} else {
		packet := BuildPacket(NOT_FOUND, nil)
		sendData(p, packet)
	}
}

//Here as well, checking open and closed block storage
func blockRes(p *peer, payload []byte) {
	var packet []byte
	var block *protocol.Block
	var blockHash [32]byte
	var blockHashWithoutTx [32]byte

	//If no specific block is requested, send latest
	if len(payload) > 0 && len(payload) == 64  {
		copy(blockHash[:], payload[:32])
		copy(blockHashWithoutTx[:], payload[32:])

		if block = storage.ReadClosedBlock(blockHash); block == nil {
			if block = storage.ReadClosedBlockWithoutTx(blockHashWithoutTx); block == nil {
				block = storage.ReadOpenBlock(blockHash)
			}
		}
	} else if len(payload) == 0 {
		block = storage.ReadLastClosedBlock()
	}

	if block != nil {
		packet = BuildPacket(BLOCK_RES, block.Encode())
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

//Response the requested block SPV header
func blockHeaderRes(p *peer, payload []byte) {
	var encodedHeader, packet []byte

	//If no specific header is requested, send latest
	if len(payload) > 0 {
		if len(payload) != 32 {
			return
		}
		var blockHash [32]byte
		copy(blockHash[:], payload[:32])
		if block := storage.ReadClosedBlock(blockHash); block != nil {
			//block.InitBloomFilter(append(storage.GetTxPubKeys(block)))
			encodedHeader = block.EncodeHeader()
		}
	} else {
		if block := storage.ReadLastClosedBlock(); block != nil {
			//block.InitBloomFilter(append(storage.GetTxPubKeys(block)))
			encodedHeader = block.EncodeHeader()
		}
	}

	if len(encodedHeader) > 0 {
		packet = BuildPacket(BlOCK_HEADER_RES, encodedHeader)
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

//Responds to an account request from another miner
func accRes(p *peer, payload []byte) {
	var packet []byte
	var hash [32]byte
	if len(payload) != 32 {
		return
	}
	copy(hash[:], payload[0:32])

	acc, _ := storage.GetAccount(hash)
	packet = BuildPacket(ACC_RES, acc.Encode())

	sendData(p, packet)
}

func rootAccRes(p *peer, payload []byte) {
	var packet []byte
	var hash [32]byte
	if len(payload) != 32 {
		return
	}
	copy(hash[:], payload[0:32])

	acc, _ := storage.GetRootAccount(hash)
	packet = BuildPacket(ROOTACC_RES, acc.Encode())

	sendData(p, packet)
}

//Completes the handshake with another miner.
func pongRes(p *peer, payload []byte, peerType uint) {
	//Payload consists of a 2 bytes array (port number [big endian encoded]).
	port := _pongRes(payload)

	if port != "" {
		p.listenerPort = port
	} else {
		p.conn.Close()
		return
	}

	//Restrict amount of connected miners
	if peers.len(PEERTYPE_MINER) >= MAX_MINERS {
		return
	}

	//Complete handshake
	var packet []byte
	if peerType == MINER_PING {
		p.peerType = PEERTYPE_MINER
		packet = BuildPacket(MINER_PONG, nil)
	} else if peerType == CLIENT_PING {
		p.peerType = PEERTYPE_CLIENT
		packet = BuildPacket(CLIENT_PONG, nil)
	}

	go peerConn(p)

	sendData(p, packet)
}

//Decouple the function for testing.
func _pongRes(payload []byte) string {
	if len(payload) == PORT_SIZE {
		return strconv.Itoa(int(binary.BigEndian.Uint16(payload[0:PORT_SIZE])))
	} else {
		return ""
	}
}

func neighborRes(p *peer) {
	//only supporting ipv4 addresses for now, makes fixed-size structure easier
	//in the future following structure is possible:
	//1) nr of ipv4 addresses, 2) nr of ipv6 addresses, followed by list of both
	var packet []byte
	var ipportList []string
	peerList := peers.getAllPeers(PEERTYPE_MINER)

	for _, p := range peerList {
		ipportList = append(ipportList, p.getIPPort())
	}

	packet = BuildPacket(NEIGHBOR_RES, _neighborRes(ipportList))
	sendData(p, packet)
}

//Decouple functionality to facilitate testing
func _neighborRes(ipportList []string) (payload []byte) {

	payload = make([]byte, len(ipportList)*6) //6 = size of ipv4 address + port
	index := 0
	for _, ipportIter := range ipportList {
		ipport := strings.Split(ipportIter, ":")
		split := strings.Split(ipport[0], ".")

		//Serializing IP:Port addr tuples
		for ipv4addr := 0; ipv4addr < 4; ipv4addr++ {
			addrPart, err := strconv.Atoi(split[ipv4addr])
			if err != nil {
				return nil
			}
			payload[index] = byte(addrPart)
			index++
		}

		port, _ := strconv.ParseUint(ipport[1], 10, 16)

		//serialize port number
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, port)
		payload[index] = buf.Bytes()[len(buf.Bytes())-2]
		index++
		payload[index] = buf.Bytes()[len(buf.Bytes())-1]
		index++
	}

	return payload
}

func intermediateNodesRes(p *peer, payload []byte) {
	var blockHash, txHash [32]byte
	var nodeHashes [][]byte
	var packet []byte
	if len(payload) != 64 {
		return
	}
	copy(blockHash[:], payload[:32])
	copy(txHash[:], payload[32:64])

	merkleTree := protocol.BuildMerkleTree(storage.ReadClosedBlock(blockHash))

	if intermediates, _ := protocol.GetIntermediate(protocol.GetLeaf(merkleTree, txHash)); intermediates != nil {
		for _, node := range intermediates {
			nodeHashes = append(nodeHashes, node.Hash[:])
		}

		packet = BuildPacket(INTERMEDIATE_NODES_RES, protocol.Encode(nodeHashes, 32))
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

func shardBlockRes(p *peer, payload []byte) {
	var packet []byte
	var b *protocol.Block

	strPayload := string(payload)
	shardID,_ := strconv.ParseInt(strings.Split(strPayload,":")[0],10,64)

	height,_ := strconv.ParseInt(strings.Split(strPayload,":")[1],10,64)

	if storage.IsCommittee {
		packet = BuildPacket(NOT_FOUND, nil)
		sendData(p,packet)
		return
	}

	logger.Printf("responding block request for shard %d for height: %d\n from peer: %s", shardID, height, p.getIPPort())

	//security check becuase the listener to incoming blocks is a concurrent goroutine
	if storage.ReadLastClosedEpochBlock() == nil || storage.ReadLastClosedBlock() == nil {
		logger.Printf("Haven't stored last epoch block yet.")
		packet = BuildPacket(NOT_FOUND, nil)
	} else {
		b = storage.ReadLastClosedBlock()
		//block cannot be nil or the genesis block
		if b != nil && b.Height != 1 && int(b.Height) == int(height) && b.ShardId == int(shardID) {
			logger.Printf("Responding Shard Block Request with block height: %d from shard ID: %d", b.Height, shardID)
			packet = BuildPacket(SHARD_BLOCK_RES, b.Encode())
			sendData(p, packet)
			return
		} else {
			if b == nil {
				logger.Printf("Last closed block is nil")
				packet = BuildPacket(NOT_FOUND, nil)
				sendData(p,packet)
				return
			}
			logger.Printf("Last closed block height: %d and shard ID: %d", b.Height, b.ShardId)
			packet = BuildPacket(NOT_FOUND, nil)
		}
	}
	sendData(p, packet)
}

func TransactionAssignmentRes(p *peer, payload []byte) {
	var packet []byte
	var ta *protocol.TransactionAssignment

	//prepare packet
	packet = BuildPacket(NOT_FOUND, nil)

	strPayload := string(payload)
	shardID, _ := strconv.ParseInt(strings.Split(strPayload, ":")[0], 10, 64)

	height, _ := strconv.ParseInt(strings.Split(strPayload, ":")[1], 10, 64)

	if !storage.IsCommittee {
		packet = BuildPacket(NOT_FOUND, nil)
		sendData(p, packet)
		return
	} else {
		//only the leader answers the request
		if storage.CommitteeLeader == protocol.SerializeHashContent(storage.ValidatorAccAddress) {
			logger.Printf("responding transaction assignment request for shard %d for height: %d\n", shardID, height)
			//security check becuase the listener to incoming blocks is a concurrent goroutine
			if storage.ReadLastClosedEpochBlock() == nil {
				logger.Printf("Haven't stored last epoch block yet.")
				packet = BuildPacket(NOT_FOUND, nil)
			} else if storage.AssignmentHeight == int(height) {
				ta = storage.AssignedTxMap[int(shardID)]
				if ta == nil {
					logger.Printf("Error: Can't find Assignment at Height: %d", height)
					packet = BuildPacket(NOT_FOUND, nil)
					sendData(p, packet)
					return
				}
				logger.Printf("responding assignment. Just read it from map. ShardID: %d Height: %d", shardID, height)
				packet = BuildPacket(TRANSACTION_ASSIGNMENT_RES, ta.EncodeTransactionAssignment())
			}
		} else {
			packet = BuildPacket(NOT_FOUND, nil)
		}
		sendData(p, packet)

	}
}

func CommitteeCheckRes(p *peer, payload []byte) {
	var packet []byte
	var cc *protocol.CommitteeCheck

	//prepare packet
	packet = BuildPacket(NOT_FOUND, nil)

	//only committee responds to this request
	if !storage.IsCommittee {
		packet = BuildPacket(NOT_FOUND, nil)
		sendData(p, packet)
		return
	}

	if storage.ReadLastClosedEpochBlock() == nil {
		logger.Printf("Haven't stored the last Epoch Block yet.")
		packet = BuildPacket(NOT_FOUND, nil)
		sendData(p,packet)
		return
	}

	logger.Printf("Committee Check Request received. Answering...")

	strPayload := string(payload)
	address := strings.Split(strPayload, ":")[0]
	height, _ := strconv.ParseInt(strings.Split(strPayload, ":")[1], 10, 64)

	validatorAccAddress := protocol.SerializeHashContent(storage.ValidatorAccAddress)

	//we reached the right height
	if storage.AssignmentHeight == int(height) && string(validatorAccAddress[:]) == address  {
		cc = storage.OwnCommitteeCheck
		logger.Printf("Sending committee check request answer for height: %d", cc.Height)
		packet = BuildPacket(COMMITTEE_CHECK_RES, cc.EncodeCommitteeCheck())
	} else {
		logger.Printf("Assignment height: %d vs Request height: %d", storage.AssignmentHeight, height)
	}

	sendData(p, packet)

}


func stateTransitionRes(p *peer, payload []byte) {
	var packet []byte
	var st *protocol.StateTransition

	//prepare packet
	packet = BuildPacket(NOT_FOUND, nil)
	strPayload := string(payload)
	shardID,_ := strconv.ParseInt(strings.Split(strPayload,":")[0],10,64)

	height,_ := strconv.ParseInt(strings.Split(strPayload,":")[1],10,64)

	logger.Printf("responding state transition request for shard %d for height: %d\n", shardID, height)

	//security check becuase the listener to incoming blocks is a concurrent goroutine
	 if storage.ReadLastClosedEpochBlock() == nil {
		logger.Printf("Haven't stored last epoch block yet.")
		packet = BuildPacket(NOT_FOUND,nil)
	} else {
		//check if the transition is from last block before epoch block.
		//this actually gives us a lot of trouble, as the shard IDs might have been switched around.
		if height == int64(storage.ReadLastClosedEpochBlock().Height-1) {
			logger.Printf("Got a request for a transition before the epoch block. Height: %d", height)
			//check if the shard ID before the last epoch block was the same as the request. if yes, I am responsible for the transition
			if shardID == int64(storage.ThisShardMap[int(storage.ReadLastClosedEpochBlock().Height)-storage.EpochLength-1]) {
				logger.Printf("Request was from my (old) shard ID: %d", shardID)
				st = storage.ReadStateTransitionFromOwnStash(int(height))
				if st != nil {
					packet = BuildPacket(STATE_TRANSITION_RES, st.EncodeTransition())
					logger.Printf("sent state transition response for height: %d\n", height)
				} else {
					logger.Printf("the state transition should have been mine but I couldn't find it...")
					packet = BuildPacket(NOT_FOUND, nil)
				}
			} else {
				logger.Printf("Shard ID was %d, not mine", shardID)
				packet = BuildPacket(NOT_FOUND, nil)
			}
		} else {
			if shardID == int64(storage.ThisShardIDDelayed) {
				st = storage.ReadStateTransitionFromOwnStash(int(height))
				if (st != nil) {
					if int64(st.ShardID) == shardID {
						packet = BuildPacket(STATE_TRANSITION_RES, st.EncodeTransition())
						logger.Printf("sent state transition response for height: %d\n", height)
					}
				} else {
					packet = BuildPacket(NOT_FOUND, nil)
					logger.Printf("state transition for height %d was nil.\n", height)
				}
			} else {
				logger.Printf("Shard ID was %d, not mine", shardID)
				packet = BuildPacket(NOT_FOUND, nil)
			}
		}
	}
	sendData(p, packet)

}

func genesisRes(p *peer, payload []byte) {
	var packet []byte
	genesis, err := storage.ReadGenesis()
	if err == nil && genesis != nil {
		packet = BuildPacket(GENESIS_RES, genesis.Encode())
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

func FirstEpochBlockRes(p *peer, payload []byte) {
	var packet []byte
	firstEpochBlock, err := storage.ReadFirstEpochBlock()

	if err == nil && firstEpochBlock != nil {
		packet = BuildPacket(FIRST_EPOCH_BLOCK_RES, firstEpochBlock.Encode())
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

func LastEpochBlockRes(p *peer, payload []byte) {
	var packet []byte

	var lastEpochBlock *protocol.EpochBlock
	lastEpochBlock = storage.ReadLastClosedEpochBlock()

	if lastEpochBlock != nil {
		packet = BuildPacket(LAST_EPOCH_BLOCK_RES, lastEpochBlock.Encode())
	} else {
		packet = BuildPacket(NOT_FOUND, nil)
	}

	sendData(p, packet)
}

func EpochBlockRes(p *peer, payload []byte) {
	var ebHash [32]byte
	copy(ebHash[:], payload[0:32])

	var eb *protocol.EpochBlock
	closedEb := storage.ReadClosedEpochBlock(ebHash)

	if closedEb != nil {
		eb = closedEb
	}

	if eb == nil {
		packet := BuildPacket(NOT_FOUND, nil)
		sendData(p, packet)
		return
	}

	var packet []byte
	packet = BuildPacket(EPOCH_BLOCK_RES, eb.Encode())

	sendData(p, packet)
}

