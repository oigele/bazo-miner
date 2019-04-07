package miner

import (
	"encoding/binary"
	"github.com/bazo-blockchain/bazo-miner/p2p"
	"github.com/bazo-blockchain/bazo-miner/protocol"
	"github.com/bazo-blockchain/bazo-miner/storage"
	"sort"
	"time"
)

//The code here is needed if a new block is built. All open (not yet validated) transactions are first fetched
//from the mempool and then sorted. The sorting is important because if transactions are fetched from the mempool
//they're received in random order (because it's implemented as a map). However, if a user wants to issue more fundsTxs
//they need to be sorted according to increasing txCnt, this greatly increases throughput.

type openTxs []protocol.Transaction

var (
	receivedBlockInTheMeantime 	bool
	nonAggregatableTxCounter 	int
	blockSize					int
	transactionHashSize			int
)


func prepareBlock(block *protocol.Block) {
	//Fetch all txs from mempool (opentxs).
	opentxs := storage.ReadAllOpenTxs()
	opentxs = append(opentxs, storage.ReadAllINVALIDOpenTx()...)
	var opentxToAdd []protocol.Transaction

	//This copy is strange, but seems to be necessary to leverage the sort interface.
	//Shouldn't be too bad because no deep copy.
	var tmpCopy openTxs
	tmpCopy = opentxs
	sort.Sort(tmpCopy)

	nonAggregatableTxCounter = 0 //Counter for all transactions which will not be aggregated. (Stake-, config-, acctx)
	blockSize = int(activeParameters.Block_size) - (650 + 8) //Set blocksize - (fixed space + Bloomfiltersize
	logger.Printf("block.GetBloomFilterSize() %v", block.GetBloomFilterSize())
	transactionHashSize = 32  //It is 32 bytes

	//map where all senders from FundsTx and AggTx are added to. --> this ensures that tx with same sender are only counted once.
	storage.DifferentSenders = map[[32]byte]uint32{}
	storage.DifferentReceivers = map[[32]byte]uint32{}
	storage.FundsTxBeforeAggregation = nil

	type senderTxCounterForMissingTransactions struct {
		senderAddress [32]byte
		txcnt uint32
		missingTransactions []uint32
	}

	var missingTxCntSender = map[[32]byte]*senderTxCounterForMissingTransactions{}

	//Get Best combination of transactions
	//opentxToAdd = checkBestCombination(opentxs)

	txCounter := 0
	for _, tx := range opentxs {
		//Prevent block size to overflow.
		txCounter++
		if (txCounter*32) > blockSize {
			break
		} else {
			opentxToAdd = append(opentxToAdd, tx)
		}
	}

	//Search missing transactions for the transactions which will be added...
	for _, tx := range opentxToAdd {
		switch tx.(type) {
		case *protocol.FundsTx:
			trx := tx.(*protocol.FundsTx)

			//Create Mininmal txCnt for the different senders with stateTxCnt.. This is used to fetch missing transactions later on.
			if missingTxCntSender[trx.From] == nil {
				if storage.State[trx.From] != nil {
					if storage.State[trx.From].TxCnt == 0 {
						missingTxCntSender[trx.From] = &senderTxCounterForMissingTransactions{trx.From, 0, nil}
					} else {
						missingTxCntSender[trx.From] = &senderTxCounterForMissingTransactions{trx.From, storage.State[trx.From].TxCnt - 1, nil}
					}
				}
			}

			if missingTxCntSender[trx.From] != nil {
				for i := missingTxCntSender[trx.From].txcnt + 1; i < trx.TxCnt; i++ {
					if i == 1 {
						missingTxCntSender[trx.From].missingTransactions = append(missingTxCntSender[trx.From].missingTransactions, 0)
					}
					missingTxCntSender[trx.From].missingTransactions = append(missingTxCntSender[trx.From].missingTransactions, i)
				}

				if trx.TxCnt > missingTxCntSender[trx.From].txcnt {
					missingTxCntSender[trx.From].txcnt = trx.TxCnt
				}
			}
		}
	}

	//Special Request for transactions missing between the Tx with the lowest TxCnt and the state.
	// With this transactions may are validated quicker.
	for _, sender := range missingTxCntSender {

		//This limits the searching process to teh block interval * TX_FETCH_TIMEOUT
		if len(missingTxCntSender[sender.senderAddress].missingTransactions) > int(activeParameters.Block_interval) {
			missingTxCntSender[sender.senderAddress].missingTransactions = missingTxCntSender[sender.senderAddress].missingTransactions[0:int(activeParameters.Block_interval)]
		}

		if len(missingTxCntSender[sender.senderAddress].missingTransactions) > 0 {
			logger.Printf("Missing Transaction: All these Transactions are missing for sender %x: %v ", sender.senderAddress[0:8], missingTxCntSender[sender.senderAddress].missingTransactions)
		}

		for _, missingTxcnt := range missingTxCntSender[sender.senderAddress].missingTransactions {

			var missingTransaction protocol.Transaction

			//Abort requesting if a block is received in the meantime
			if receivedBlockInTheMeantime {
				logger.Printf("Received Block in the Meantime --> Abort requesting missing Tx (1)")
				break
			}

			//Search Tx in the local storage, if it may is received in the meantime.
			for _, txhash := range storage.ReadTxcntToTx(missingTxcnt) {
				tx := storage.ReadOpenTx(txhash)
				if tx != nil {
					if tx.Sender() == sender.senderAddress {
						missingTransaction = tx
						break
					}
				} else {
					tx = storage.ReadINVALIDOpenTx(txhash)
					if tx != nil {
						if tx.Sender() == sender.senderAddress {
							missingTransaction = tx
							break
						}
					} else {
						tx = storage.ReadClosedTx(txhash)
						if tx != nil {
							if tx.Sender() == sender.senderAddress {
								missingTransaction = tx
								break
							}
						}
					}
				}
			}

			//Try to fetch the transaction form the network, if it is not received until now.
			if missingTransaction == nil {
				var requestTx = specialTxRequest{sender.senderAddress, p2p.SPECIALTX_REQ, missingTxcnt}
				payload := requestTx.Encoding()
				//Special Request can be received through the fundsTxChan.
				err := p2p.TxWithTxCntReq(payload, p2p.SPECIALTX_REQ)
				if err != nil {
					continue
				}
				select {
				case trx := <-p2p.FundsTxChan:
					//If correct transaction is received, write to openStorage and good, if wrong one is received, break.
					if trx.TxCnt != missingTxcnt && trx.From != sender.senderAddress {
						logger.Printf("Missing Transaction: Received Wrong Transaction")
						break
					} else {
						storage.WriteOpenTx(trx)
						missingTransaction = trx
						break
					}
				case <-time.After(TXFETCH_TIMEOUT * time.Second):
					stash := p2p.ReceivedFundsTXStash
					//Try to find missing transaction in the stash...
					for _, trx := range stash {
						if trx.From == sender.senderAddress && trx.TxCnt == missingTxcnt {
							storage.WriteOpenTx(trx)
							missingTransaction = trx
							break
						}
					}

					if missingTransaction == nil {
						logger.Printf("Missing Transaction: Tx Request Timed out...")
					}
					break
				}
			}

			if missingTransaction == nil {
				logger.Printf("Missing txcnt %v not found", missingTxcnt)
			} else {
				opentxToAdd = append(opentxToAdd, missingTransaction)
			}
		}
		//If Block is receifed before, break now.
		if receivedBlockInTheMeantime {
			logger.Printf("Received Block in the Meantime --> Abort requesting missing Tx (2)")
			receivedBlockInTheMeantime = false
			break
		}
	}

	missingTxCntSender = nil
	//Sort Tx Again to get lowest TxCnt at the beginning.
	tmpCopy = opentxToAdd
	sort.Sort(tmpCopy)

	//Add previous selected transactions.
	for _, tx := range opentxToAdd {
		err := addTx(block, tx)
		if err != nil {
			//If the tx is invalid, we remove it completely, prevents starvation in the mempool.
			storage.DeleteOpenTx(tx)
		}
	}

	// In miner\block.go --> AddFundsTx the transactions get added into storage.TxBeforeAggregation.
	if len(storage.ReadFundsTxBeforeAggregation()) > 0 {
		splitSortedAggregatableTransactions(block)
	}

	//Set measurement values back to zero / nil.
	storage.DifferentSenders = nil
	storage.DifferentReceivers = nil
	nonAggregatableTxCounter = 0
	return
}

func checkBestCombination(openTxs []protocol.Transaction) (TxToAppend []protocol.Transaction) {
	nrWhenCombinedBest := 0
	moreOpenTx := true
	for moreOpenTx {
		var intermediateTxToAppend []protocol.Transaction

		for i, tx := range openTxs {
			switch tx.(type) {
			case *protocol.FundsTx:
				storage.DifferentSenders[tx.(*protocol.FundsTx).From] = storage.DifferentSenders[tx.(*protocol.FundsTx).From] + 1
				storage.DifferentReceivers[tx.(*protocol.FundsTx).To] = storage.DifferentReceivers[tx.(*protocol.FundsTx).To] + 1
			case *protocol.AggTx:
				continue
			default:
				//If another non-FundsTx can fit into the block, add it, else block is already full, so return the tx
				//This does help that non-FundsTx get validated as fast as possible.
				if (nonAggregatableTxCounter+1)*transactionHashSize < blockSize {
					nonAggregatableTxCounter += 1
					TxToAppend = append(TxToAppend, tx)
					openTxs = append(openTxs[:i], openTxs[i+1:]...)
				} else {
					return TxToAppend
				}
			}
		}

		maxSender, addressSender := getMaxKeyAndValueFormMap(storage.DifferentSenders)
		maxReceiver, addressReceiver := getMaxKeyAndValueFormMap(storage.DifferentReceivers)

		i := 0
		if maxSender >= maxReceiver {
			for _, tx := range openTxs {
				switch tx.(type) {
				case *protocol.FundsTx:
					//Append Tx To the ones which get added, else remove added tx such that no space exists.
					if tx.(*protocol.FundsTx).From == addressSender {
						intermediateTxToAppend = append(intermediateTxToAppend, tx)
					} else {
						openTxs[i] = tx
						i++
					}
				}
			}
		} else {
			for _, tx := range openTxs {
				switch tx.(type) {
				case *protocol.FundsTx:
					if tx.(*protocol.FundsTx).To == addressReceiver {
						intermediateTxToAppend = append(intermediateTxToAppend, tx)
					} else {
						openTxs[i] = tx
						i++
					}
				}
			}
		}
		openTxs = openTxs[:i]
		storage.DifferentSenders = make(map[[32]byte]uint32)
		storage.DifferentReceivers = make(map[[32]byte]uint32)

		nrWhenCombinedBest = nrWhenCombinedBest + 1

		//Stop when block is full
		if (nrWhenCombinedBest+nonAggregatableTxCounter)*transactionHashSize >= blockSize {
			moreOpenTx = false
			break
		} else {
			TxToAppend = append(TxToAppend, intermediateTxToAppend...)
		}

		//Stop when list is empty
		if len(openTxs) > 0 {
			//If adding a new transaction combination, gets bigger than the blocksize, abort
			moreOpenTx = true
		} else {
			moreOpenTx = false
		}
	}

	return TxToAppend
}

type specialTxRequest struct {
	senderHash [32]byte
	reqType    uint8
	txcnt      uint32
}

func (R *specialTxRequest) Encoding() (encodedTx []byte) {

	// Encode
	if R == nil {
		return nil
	}
	var txcnt [8]byte
	binary.BigEndian.PutUint32(txcnt[:], R.txcnt)
	encodedTx = make([]byte, 42)

	encodedTx[0] = R.reqType
	copy(encodedTx[1:9], txcnt[:])
	copy(encodedTx[10:42], R.senderHash[:])

	return encodedTx
}

//Implement the sort interface
func (f openTxs) Len() int {
	return len(f)
}

func (f openTxs) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f openTxs) Less(i, j int) bool {
	//Comparison only makes sense if both tx are fundsTxs.
	//Why can we only do that with switch, and not e.g., if tx.(type) == ..?
	switch f[i].(type) {
	case *protocol.AccTx:
		//We only want to sort a subset of all transactions, namely all fundsTxs.
		//However, to successfully do that we have to place all other txs at the beginning.
		//The order between accTxs and configTxs doesn't matter.
		return true
	case *protocol.ConfigTx:
		return true
	case *protocol.StakeTx:
		return true
	case *protocol.AggTx:
		return true
	}

	switch f[j].(type) {
	case *protocol.AccTx:
		return false
	case *protocol.ConfigTx:
		return false
	case *protocol.StakeTx:
		return false
	case *protocol.AggTx:
		return false
	}

	return f[i].(*protocol.FundsTx).TxCnt < f[j].(*protocol.FundsTx).TxCnt
}