package p2p

import (
	"errors"
)

//Both block and tx requests are handled asymmetricaly, using channels as inter-communication
//All the request in this file are specifically initiated by the miner package
//func BlockReq(hash [32]byte, hashWithoutTx [32]byte) error {
func BlockReq(hash [32]byte, hashWithoutTx [32]byte) error {

	payload := hash[:]
	payloadTEMP := hashWithoutTx[:]

	payload = append(payload, payloadTEMP...)

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

func LastBlockReq() error {

	p := peers.getRandomPeer(PEERTYPE_MINER)
	if p == nil {
		return errors.New("Couldn't get a connection, request not transmitted.")
	}

	packet := BuildPacket(BLOCK_REQ, nil)
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
func TxWithTxCntReq(payload []byte, reqType uint8) error { //TODO

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

func PrintMinerCons() {
	minerConnections := peers.getAllPeers(PEERTYPE_MINER)
	logger.Printf(" ____________")
	logger.Printf("| Neighbors: |__________________")
	if len(minerConnections) > 0 {
		for _, p := range minerConnections {
			logger.Printf("|-- Miner: %v", p.getIPPort())
		}
	} else {
		logger.Printf("|   No Neighbors                |", )
	}
	logger.Printf("|_______________________________|")
}
