package p2p

import (
	"errors"
	"strconv"
	"time"
)

//legacy code
func BlockReq(hash [32]byte, hashWithoutTx [32]byte) error {

	logger.Printf("Request Block %x, %x from the network (%v Miners)", hash[0:8], hashWithoutTx[0:8], peers.len(PEERTYPE_MINER))

	payload := hash[:]
	payloadTEMP := hashWithoutTx[:]

	payload = append(payload, payloadTEMP...)

	if peers.len(PEERTYPE_MINER) == 0 {
		wait := true
		for wait {
			time.Sleep(2*time.Second)
			logger.Printf("Currently no other miners around... Waiting to request block...")
			if peers.len(PEERTYPE_MINER) > 0 {
				wait = false
			}
		}
	}

	// Block Request with a Broadcast request. This does rise the possibility of a valid answer.
	for _, p := range peers.getAllPeers(PEERTYPE_MINER) {
		//Write to the channel, which the peerBroadcast(*peer) running in a seperate goroutine consumes right away.

		if p == nil {
			return errors.New("Couldn't get a connection, request not transmitted.")
		}
		packet := BuildPacket(BLOCK_REQ, payload)
		sendData(p, packet)
	}

	return nil
}

//the committe node might have to request blocks from the network. shard blocks are uniquely identified with their block height and shard ID
func ShardBlockReq(height int, shardID int) {
	strShardID := strconv.Itoa(shardID)
	strHeight := strconv.Itoa(height)

	strRequest := ""
	strRequest += strShardID
	strRequest += ":"
	strRequest += strHeight

	ShardBlockShardOut <- []byte(strRequest)
}

func TransactionAssignmentReq(height int, shardID int) {
	strShardID := strconv.Itoa(shardID)
	strHeight := strconv.Itoa(height)

	strRequest := ""
	strRequest += strShardID
	strRequest += ":"
	strRequest += strHeight

	TransactionAssignmentReqOut <- []byte(strRequest)
}

func FirstEpochBlockReq() error {
	p := peers.getRandomPeer(PEERTYPE_MINER)
	if p == nil {
		return errors.New("Couldn't get a connection, request not transmitted.")
	}

	packet := BuildPacket(FIRST_EPOCH_BLOCK_REQ, nil)
	sendData(p, packet)
	return nil
}

func LastEpochBlockReq() error {
	p := peers.getRandomPeer(PEERTYPE_MINER)
	if p == nil {
		return errors.New("Couldn't get a connection, request not transmitted.")
	}

	packet := BuildPacket(LAST_EPOCH_BLOCK_REQ, nil)
	sendData(p, packet)
	return nil
}

func LastBlockReq() error {

	p := peers.getRandomPeer(PEERTYPE_MINER)
	if p == nil {
		return errors.New("Couldn't get a connection, request not transmitted.")
	}

	packet := BuildPacket(BLOCK_REQ, nil)
	sendData(p, packet)
	return nil
}

func GenesisReq() error {
	p := peers.getRandomPeer(PEERTYPE_MINER)
	if p == nil {
		return errors.New("Couldn't get a connection, request not transmitted.")
	}

	packet := BuildPacket(GENESIS_REQ, nil)
	sendData(p, packet)
	return nil
}

//Request specific transaction
func TxReq(hash [32]byte, reqType uint8) error {

	// Tx Request also as brodcast so that the possibility of an answer is higher.
	for _, p := range peers.getAllPeers(PEERTYPE_MINER) {
		//Write to the channel, which the peerBroadcast(*peer) running in a seperate goroutine consumes right away.

		if p == nil {
			return errors.New("Couldn't get a connection, request not transmitted.")
		}
		packet := BuildPacket(reqType, hash[:])
		sendData(p, packet)
	}

	return nil
}

//Request specific transaction
func TxWithTxCntReq(payload []byte, reqType uint8) error {

	// Tx Request also as brodcast so that the possibility of an answer is higher.
	for _, p := range peers.getAllPeers(PEERTYPE_MINER) {
		//Write to the channel, which the peerBroadcast(*peer) running in a seperate goroutine consumes right away.

		if p == nil {
			return errors.New("Couldn't get a connection, request not transmitted.")
		}

		packet := BuildPacket(reqType, payload)
		sendData(p, packet)
	}

	return nil
}

func PrintMinerConns() {
	minerConnections := peers.getAllPeers(PEERTYPE_MINER)
	logger.Printf("  ____________")
	logger.Printf(" | Neighbors: |__________________")
	if len(minerConnections) > 0 {
		for _, p := range minerConnections {
			logger.Printf(" |-- Miner: %v", p.getIPPort())
		}
	} else {
		logger.Printf("|   No Neighbors                |", )
	}
	logger.Printf("|_______________________________|")
}

func StateTransitionReqShard(shardID int,height int) {
	strShardID := strconv.Itoa(shardID)
	strHeight := strconv.Itoa(height)

	strRequest := ""
	strRequest += strShardID
	strRequest += ":"
	strRequest += strHeight

	StateTransitionShardOut <- []byte(strRequest)
}

func CommitteeCheckReq(address [32]byte, height int) {
	strSender := string(address[:])
	strHeight := strconv.Itoa(height)

	strRequest := ""
	strRequest += strSender
	//can use : as delimiter because addresses don't contain :
	strRequest += ":"
	strRequest += strHeight


	CommitteeCheckShardOut <- []byte(strRequest)
}