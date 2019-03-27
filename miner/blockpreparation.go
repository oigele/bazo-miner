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

var receivedBlockInTheMeantime bool

func prepareBlock(block *protocol.Block) {
	//Fetch all txs from mempool (opentxs).
	opentxs := storage.ReadAllOpenTxs()
	//opentxs = append(opentxs, storage.ReadAllINVALIDOpenTx()...)
	var opentxToAdd []protocol.Transaction

	//This copy is strange, but seems to be necessary to leverage the sort interface.
	//Shouldn't be too bad because no deep copy.
	var tmpCopy openTxs
	tmpCopy = opentxs
	sort.Sort(tmpCopy)

	nonAggregatableTxCounter := 0 //Counter for all transactions which will not be aggregated. (Stake-, config-, acctx)
	transactionCounter := 0 //Counter for all transactions which will be aggregated. (Funds-, AggTx)

	blockSize := block.GetSize()+block.GetBloomFilterSize()
	transactionHashSize := 32  //It is 32 bytes

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

	//Check how many transactions can be added.
	for _, tx := range opentxs {
		//Switch because with an if statement every transaction would need a getter-method for its type.
		//Therefore, switch is more code-efficient.
		switch tx.(type) {
		case *protocol.FundsTx:
			trx := tx.(*protocol.FundsTx)
			storage.DifferentSenders[trx.From] = storage.DifferentSenders[trx.From] + 1
			storage.DifferentReceivers[trx.To] = storage.DifferentReceivers[trx.To] + 1

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
		case *protocol.AggTx:
			storage.DifferentSenders[tx.Sender()] = storage.DifferentSenders[tx.Sender()] + 1
			storage.DifferentReceivers[tx.Receiver()] = storage.DifferentReceivers[tx.Receiver()] + 1
		default:
			nonAggregatableTxCounter += 1
		}

		//The Maximum Number of transactions is always the smaller number of Different Senders or Different Receivers.
		if len(storage.DifferentSenders) <= len(storage.DifferentReceivers) {
			transactionCounter = len(storage.DifferentSenders)
		} else {
			transactionCounter = len(storage.DifferentReceivers)
		}

		//Check if block will become to big when adding the next transaction.
		if int(blockSize)+(transactionCounter+nonAggregatableTxCounter)*transactionHashSize > int(activeParameters.Block_size) {
			logger.Printf("Block Would Overflow --> Stop adding new Transactions")
			break
		} else {
			opentxToAdd = append(opentxToAdd, tx)
		}
	}



	//Special Request for transactions missing between the Tx with the lowest TxCnt and the state.
	// With this transactions may are validated quicker.
	for _, sender := range missingTxCntSender {

		if len(missingTxCntSender[sender.senderAddress].missingTransactions) > 0 {
			logger.Printf("Missing Transaction: All these Transactions are missing for sender %x: %v ", sender.senderAddress[0:8], missingTxCntSender[sender.senderAddress].missingTransactions)
		}

		for _, missingTxcnt := range missingTxCntSender[sender.senderAddress].missingTransactions {

			var missingTransaction protocol.Transaction
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
					if trx.TxCnt != missingTxcnt && trx.From != sender.senderAddress {
						logger.Printf("Missing Transaction: Received Wrong Transaction")
						break
					} else {
						storage.WriteOpenTx(trx)
						missingTransaction = trx
						break
					}
				case <-time.After(TXFETCH_TIMEOUT * time.Second):
					logger.Printf("Missing Transaction: Tx Request Timed out...")
					break
				}
			}

			if missingTransaction == nil {
				logger.Printf("Missing txcnt %v not found", missingTxcnt)
			} else {
				opentxToAdd = append(opentxToAdd, missingTransaction)
			}
		}
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