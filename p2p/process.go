package p2p

import (
	"encoding/binary"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"strconv"
	"sync"
)

var (
	processTxBroadcastMutex = &sync.Mutex{}
)

//Process tx broadcasts from other miners. We can't broadcast incoming messages directly, first check if
//the tx has already been broadcast before, whether it is a valid tx etc.
func processTxBrdcst(p *peer, payload []byte, brdcstType uint8) {

	var tx protocol.Transaction
	//Make sure the transaction can be properly decoded, verification is done at a later stage to reduce latency
	switch brdcstType {
	case FUNDSTX_BRDCST:
		var fTx *protocol.FundsTx
		fTx = fTx.Decode(payload)
		//Legacy code to muliply transactions at the miner's side
		//Sadly only works if the verification is turned off or if the sender's private key is know
		/*if fTx.Amount == 2 {
			for i := 1; i <= 100000; i++ {
				newFtx := fTx.Copy()
				newFtx.TxCnt = fTx.TxCnt + uint32(i)
				storage.WriteOpenTx(newFtx)
				fTx = newFtx
			}
			return
		}*/
		if fTx == nil {
			return
		}
		tx = fTx
	case ACCTX_BRDCST:
		var aTx *protocol.AccTx
		aTx = aTx.Decode(payload)
		if aTx == nil {
			return
		}
		tx = aTx
	case CONFIGTX_BRDCST:
		var cTx *protocol.ConfigTx
		cTx = cTx.Decode(payload)
		if cTx == nil {
			return
		}
		tx = cTx
	case STAKETX_BRDCST:
		var sTx *protocol.StakeTx
		sTx = sTx.Decode(payload)
		if sTx == nil {
			return
		}
		tx = sTx
	case AGGTX_BRDCST:
		var aTx *protocol.AggTx
		aTx = aTx.Decode(payload)
		if aTx == nil {
			return
		}
		tx = aTx
	case DATATX_BRDCST:
		var dTx *protocol.DataTx
		dTx = dTx.Decode(payload)
		if dTx == nil {
			return
		}
		tx = dTx
	case AGGDATATX_BRDCST:
		var aTx *protocol.AggDataTx
		aTx = aTx.Decode(payload)
		if aTx == nil {
			return
		}
		tx = aTx
	case COMMITTEETX_BRDCST:
		var cTx *protocol.CommitteeTx
		cTx = cTx.Decode(payload)
		if cTx == nil {
			return 
		}
		tx = cTx
	}

	//Response tx acknowledgment if the peer is a client
	//if !peers.minerConns[p] {
	if !peers.contains(p.getIPPort(), PEERTYPE_MINER) {
		packet := BuildPacket(TX_BRDCST_ACK, nil)
		sendData(p, packet)
	}

	//var fTx *protocol.FundsTx
	//fTx = fTx.Decode(payload)
	//logger.Printf("Tx from %x with count %d", fTx.From, fTx.TxCnt)


	if storage.ReadOpenTx(tx.Hash()) != nil {
		//logger.Printf("Received transaction (%x) already in the mempool.\n", tx.Hash())
		return
	}
	if storage.ReadClosedTx(tx.Hash()) != nil {
		//logger.Printf("Received transaction (%x) already validated.\n", tx.Hash())
		return
	}

	if storage.ReadOpenTxHashToDelete(tx.Hash()) == true {
		logger.Printf("stopped transaction from being added")
		return
	}


	//logger.Printf("Received Tx %x from %v", tx.Hash(), p.getIPPort())
	//Write to mempool and rebroadcast
	storage.WriteOpenTx(tx)
	//toBrdcst := BuildPacket(brdcstType, payload)
	//minerTxBrdcstMsg <- toBrdcst

}

func processTimeRes(p *peer, payload []byte) {
	time := int64(binary.BigEndian.Uint64(payload))
	//Concurrent writes need to be protected.
	//We use the same peer lock to prevent concurrent writes (on the network). It would be more efficient to use
	//different locks but the speedup is so marginal that it's not worth it.

	p.l.Lock()
	defer p.l.Unlock()
	p.time = time
}

func processNeighborRes(p *peer, payload []byte) {
	//Parse the incoming ipv4 addresses.
	ipportList := _processNeighborRes(payload)

	for _, ipportIter := range ipportList {
		//logger.Printf("IP/Port received: %v\n", ipportIter)
		//iplistChan is a buffered channel to handle ips asynchronously.
		if !peers.contains(ipportIter, PEERTYPE_MINER) && !peerSelfConn(ipportIter) && len(iplistChan) <= (MIN_MINERS * MIN_MINERS) {
			iplistChan <- ipportIter
		}
	}
}

//Split the processNeighborRes function in two for cleaner testing.
func _processNeighborRes(payload []byte) (ipportList []string) {
	index := 0

	for cnt := 0; cnt < len(payload)/(IPV4ADDR_SIZE+PORT_SIZE); cnt++ {
		var addr string
		for singleAddr := index; singleAddr < index+IPV4ADDR_SIZE; singleAddr++ {
			tmp := int(payload[singleAddr])
			addr += strconv.Itoa(tmp)
			addr += "."
		}
		//Remove trailing dot.
		addr = addr[:len(addr)-1]
		addr += ":"
		//Extract port number.
		addr += strconv.Itoa(int(binary.BigEndian.Uint16(payload[index+4 : index+6])))

		ipportList = append(ipportList, addr)
		index += IPV4ADDR_SIZE + PORT_SIZE
	}

	return ipportList
}

func processValMappingRes(p *peer, payload []byte) {
	ValidatorShardMapReq <- payload
}

func EmptyingiplistChan() {
	for i := 0; i < len(iplistChan); i++ {
		<- iplistChan
	}
}
