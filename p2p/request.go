package p2p

//Requests that the p2p package issues requesting data from other miners.
//Tx and block requests are processed in the miner_interface.go file, because
//this involves inter-communication between the two packages
func neighborReq() {

	knownPeers := peers.getAllPeers(PEERTYPE_MINER)
		for _, p := range knownPeers {
		packet := BuildPacket(NEIGHBOR_REQ, nil)
		go sendData(p, packet)
	}

}

func neighborBrdcst() {

	knownPeers := peers.getAllPeers(PEERTYPE_MINER)
	var ipportList []string
	for _, p := range knownPeers {
		ipportList = append(ipportList, p.getIPPort())

	}

	packet := BuildPacket(NEIGHBOR_RES, _neighborRes(ipportList))
	for _, p := range knownPeers {
		go sendData(p, packet)
	}

}
