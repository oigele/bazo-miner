package miner

import (
	"encoding/binary"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
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
	logger.Printf("Number of OpenTxs: %d", len(opentxs))

	for _,tx := range opentxs{
		switch tx.(type) {
		case *protocol.AccTx:
			logger.Printf("Acc Tx PubKey: (%x)", tx.(*protocol.AccTx).PubKey)
		}
	}

	var opentxToAdd []protocol.Transaction

	//This copy is strange, but seems to be necessary to leverage the sort interface.
	//Shouldn't be too bad because no deep copy.
	var tmpCopy openTxs
	tmpCopy = opentxs
	sort.Sort(tmpCopy)

	nonAggregatableTxCounter = 0                                     //Counter for all transactions which will not be aggregated. (Stake-, config-, acctx)
	blockSize = int(ActiveParameters.Block_size) - (650 + 8) //Set blocksize - (fixed space + Bloomfiltersize
	logger.Printf("block.GetBloomFilterSize() %v", block.GetBloomFilterSize())
	transactionHashSize = 32 //It is 32 bytes

	//map where all senders from FundsTx and AggTx are added to. --> this ensures that tx with same sender are only counted once.
	storage.DifferentSenders = map[[32]byte]uint32{}
	//storage.DifferentReceivers = map[[32]byte]uint32{}
	storage.FundsTxBeforeAggregation = nil

	type senderTxCounterForMissingTransactions struct {
		senderAddress       [32]byte
		txcnt               uint32
		missingTransactions []uint32
	}

	//var missingTxCntSender = map[[32]byte]*senderTxCounterForMissingTransactions{}

	//Get Best combination of transactions
	//In here, the check happens if the Tx is in the right shard

	openTxsOfShard := []protocol.Transaction{}
	for _, tx := range opentxs {
		txAssignedShard := assignTransactionToShard(tx)
		//Makes sure we only validate the transactions of our own shard
		if int(txAssignedShard) == ValidatorShardMap.ValMapping[validatorAccAddress] {
			openTxsOfShard = append(openTxsOfShard, tx)
		}
	}

	logger.Printf("length of tx assigned to shard: %d", len(openTxsOfShard))

	opentxToAdd = checkBestCombination(openTxsOfShard)

	logger.Printf("length of open tx to add with best combination: %d", len(opentxToAdd))

	/* START OF THE SEARCH ALGORITHM

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
		if len(missingTxCntSender[sender.senderAddress].missingTransactions) > int(ActiveParameters.Block_interval) {
			missingTxCntSender[sender.senderAddress].missingTransactions = missingTxCntSender[sender.senderAddress].missingTransactions[0:int(ActiveParameters.Block_interval)]
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
		//If Block is received before, break now.
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


	 */

	// END OF THE SEARCH ALGORITHM

	/* TODO evaluate if this block of code might ever be useful anymore
	//Here Kürsats Sharding Logic Begins

	//Keep track of transactions from assigned for my shard and which are valid. Only consider these ones when filling a block
	//Otherwhise we would also count invalid transactions from my shard, this prevents well-filled blocks.
	txFromThisShard := 0


	//Add previous selected transactions.
	// Here the transactions get added to the block
	for _, tx := range opentxToAdd {
		txAssignedShard := assignTransactionToShard(tx)
		logger.Printf("Assigned shard: %d", txAssignedShard)

		if int(txAssignedShard) == ValidatorShardMap.ValMapping[validatorAccAddress] {
			logger.Printf("---- Transaction (%x) in shard: %d\n", tx.Hash(), txAssignedShard)
			//Prevent block size to overflow.
			if int(block.GetSize()+10)+(txFromThisShard*int(len(tx.Hash()))) > int(activeParameters.Block_size) {
				logger.Printf("Overflow prevented")
				break
			}

			switch tx.(type) {
			case *protocol.StakeTx:
				//Add StakeTXs only when preparing the last block before the next epoch block
				if (int(lastBlock.Height) == int(lastEpochBlock.Height)+int(activeParameters.epoch_length)-1) {
					err := addTx(block, tx)
					if err == nil {
						txFromThisShard += 1
					}
				}
			case *protocol.FundsTx, *protocol.ConfigTx, *protocol.AccTx:
				err := addTx(block, tx)
				if err != nil {
					//If the tx is invalid, we remove it completely, prevents starvation in the mempool.
					//storage.DeleteOpenTx(tx)
					storage.WriteINVALIDOpenTx(tx)
				} else {
					txFromThisShard += 1
				}
			}
		}
	}

	*/
	//*********************************************************//
	//Here Kürsats Sharding Logic Ends
	//*********************************************************//


	logger.Printf("Number of OpenTxs to add right before they get added: %d", len(opentxToAdd))

	//Add previous selected transactions.
	for _, tx := range opentxToAdd {
		switch tx.(type) {
		case *protocol.StakeTx:
			if (int(lastBlock.Height) == int(lastEpochBlock.Height)+int(ActiveParameters.Epoch_length)-1) {
				err := addTx(block, tx)
				if err != nil {
					//If the tx is invalid, we remove it completely, prevents starvation in the mempool.
					storage.DeleteOpenTx(tx)
				}
			}
		default:
			err := addTx(block, tx)
			if err != nil {
				logger.Printf("Error in add tx routine: %s", err)
				//If the tx is invalid, we remove it completely, prevents starvation in the mempool.
				storage.DeleteOpenTx(tx)
			}
		}
	}

		// In miner\block.go --> AddFundsTx the transactions get added into storage.TxBeforeAggregation.
		if len(storage.ReadFundsTxBeforeAggregation()) > 0 {
			logger.Printf("Adding funds tx before aggregation")
			splitSortedAggregatableTransactions(block)
		}

		//Set measurement values back to zero / nil.
		storage.DifferentSenders = nil
		//storage.DifferentReceivers = nil
		nonAggregatableTxCounter = 0
		return
}



//Maybe needs some optimization. Note that due to our own constraint, transactions are not aggregated according to the receiver anymore, so they are not counted anymore
func checkBestCombination(openTxs []protocol.Transaction) (TxToAppend []protocol.Transaction) {
	//Explanation: While more open Txs exist and there is enough space in the block left, keep adding transactions to TxToAppend
	nrWhenCombinedBest := 0
	moreOpenTx := true
	for moreOpenTx {
		var intermediateTxToAppend []protocol.Transaction
		for i, tx := range openTxs {
			// not really appending anything here. This is just legacy code, counting how many different senders we have (in case it needs to be compared to different receivers)
			switch tx.(type) {
			case *protocol.FundsTx:
				storage.DifferentSenders[tx.(*protocol.FundsTx).From] = storage.DifferentSenders[tx.(*protocol.FundsTx).From] + 1
				//storage.DifferentReceivers[tx.(*protocol.FundsTx).To] = storage.DifferentReceivers[tx.(*protocol.FundsTx).To] + 1
			case *protocol.AggTx:
				continue
			default:
				//If another non-FundsTx can fit into the block, add it, else block is already full, so return the tx
				//This does help that non-FundsTx get validated as fast as possible.
				if (nonAggregatableTxCounter+1)*transactionHashSize < blockSize {
					nonAggregatableTxCounter += 1
					TxToAppend = append(TxToAppend, tx)
					//openTxs is shrinking because element i might be excluded. as soon as i is larger than the length of remaining slice, stop.
					//They are cut out because they
					if i < len(openTxs){
						//this excludes element i from the slice. Selects access a half-open range which includes the first element but not the last one
						openTxs = append(openTxs[:i], openTxs[i+1:]...)
					}
				} else {
					return TxToAppend
				}
			}
		}


		//TODO realfix hotfix! Remove duplicates:
		var newOpenTxs []protocol.Transaction
		duplicateChecker := make(map[[64]byte]protocol.Transaction)
		for _,tx1 := range openTxs {
			switch tx1.(type) {
			case *protocol.AccTx:
				//we dont have it yet
				_,exists := duplicateChecker[tx1.(*protocol.AccTx).PubKey]
				if !exists {
					logger.Printf("Pubkey not existent yet: (%x)", tx1.(*protocol.AccTx).PubKey)
					duplicateChecker[tx1.(*protocol.AccTx).PubKey] = tx1
					newOpenTxs = append(newOpenTxs, tx1)
				} else {
					logger.Printf("the previous routine failed. Found a duplicate")
					continue
				}
			default:
				newOpenTxs = append(newOpenTxs, tx1)
				continue
			}
		}

 		openTxs = newOpenTxs

 		//End of hotfix

		//first return value maxSender not needed anymore. We dont need to compare max sender and max receiver anymore.
		//this is still useful because we aggregate the senders with the most transactions first
		_, addressSender := getMaxKeyAndValueFormMap(storage.DifferentSenders)
		//maxReceiver, addressReceiver := getMaxKeyAndValueFormMap(storage.DifferentReceivers)

		i := 0
		//if maxSender >= maxReceiver {
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
		/*} else {
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
		}*/
		openTxs = openTxs[:i]
		storage.DifferentSenders = make(map[[32]byte]uint32)
		//storage.DifferentReceivers = make(map[[32]byte]uint32)

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

//Begin Code from Kürsat
/**
Transactions are sharded based on the public address of the sender
*/
func assignTransactionToShard(transaction protocol.Transaction) (shardNr int) {
	//Convert Address/Issuer ([64]bytes) included in TX to bigInt for the modulo operation to determine the assigned shard ID.
	switch transaction.(type) {
	case *protocol.FundsTx:
		var byteToConvert [32]byte
		byteToConvert = transaction.(*protocol.FundsTx).From
		var calculatedInt int
		calculatedInt = int(binary.BigEndian.Uint64(byteToConvert[:8]))
		return int((Abs(int32(calculatedInt)) % int32(NumberOfShards)) + 1)
	case *protocol.ConfigTx:
		var byteToConvert [64]byte
		byteToConvert = transaction.(*protocol.ConfigTx).Sig
		var calculatedInt int
		calculatedInt = int(binary.BigEndian.Uint64(byteToConvert[:8]))
		return int((Abs(int32(calculatedInt)) % int32(NumberOfShards)) + 1)
	case *protocol.StakeTx:
		var byteToConvert [32]byte
		byteToConvert = transaction.(*protocol.StakeTx).Account
		var calculatedInt int
		calculatedInt = int(binary.BigEndian.Uint64(byteToConvert[:8]))
		return int((Abs(int32(calculatedInt)) % int32(NumberOfShards)) + 1)
	default:
		return 1 // default shard ID
	}
}

func Abs(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

/**
During the synchronisation phase at every block height, the validator also receives the transaction hashes which were validated
by the other shards. To avoid starvation, delete those transactions from the mempool
*/
func DeleteTransactionFromMempool(contractData [][32]byte, fundsData [][32]byte, configData [][32]byte, stakeData [][32]byte, aggTxData[][32]byte) {
	for _,fundsTX := range fundsData{
		if(storage.ReadOpenTx(fundsTX) != nil){
			storage.WriteClosedTx(storage.ReadOpenTx(fundsTX))
			storage.DeleteOpenTx(storage.ReadOpenTx(fundsTX))
			logger.Printf("Deleted transaction (%x) from the MemPool.\n",fundsTX)
		}
	}

	for _,configTX := range configData{
		if(storage.ReadOpenTx(configTX) != nil){
			storage.WriteClosedTx(storage.ReadOpenTx(configTX))
			storage.DeleteOpenTx(storage.ReadOpenTx(configTX))
			logger.Printf("Deleted transaction (%x) from the MemPool.\n",configTX)
		}
	}

	for _,stakeTX := range stakeData{
		if(storage.ReadOpenTx(stakeTX) != nil){
			storage.WriteClosedTx(storage.ReadOpenTx(stakeTX))
			storage.DeleteOpenTx(storage.ReadOpenTx(stakeTX))
			logger.Printf("Deleted transaction (%x) from the MemPool.\n",stakeTX)
		}
	}

	for _,contractTX := range contractData{
		if(storage.ReadOpenTx(contractTX) != nil){
			storage.WriteClosedTx(storage.ReadOpenTx(contractTX))
			storage.DeleteOpenTx(storage.ReadOpenTx(contractTX))
			logger.Printf("Deleted transaction (%x) from the MemPool.\n",contractTX)
		}
	}

	//here, the AggTX only carries the hashes of the transactions that should be deleted
	for _,TX := range aggTxData {
		if (storage.ReadOpenTx(TX) != nil) {
			aggTx := storage.ReadOpenTx(TX)
			storage.WriteClosedTx(storage.ReadOpenTx(TX))
			storage.DeleteOpenTx(storage.ReadOpenTx(TX))
			logger.Printf("Deleted transaction (%x) from the MemPool. \n",TX)
			for _,fundsTX := range aggTx.(*protocol.AggTx).AggregatedTxSlice {
				if(storage.ReadOpenTx(fundsTX) != nil){
					storage.WriteClosedTx(storage.ReadOpenTx(fundsTX))
					storage.DeleteOpenTx(storage.ReadOpenTx(fundsTX))
				}
			}
		} else {
			var aggTx protocol.Transaction
			//Aggregated Transaction need to be fetched from the network.
			cnt := 0
			HERE:
			logger.Printf("Request AGGTX: %x", TX)
			err := p2p.TxReq(TX, p2p.AGGTX_REQ)
			if err != nil {
				logger.Printf("Could not fetch AggTX")
				return
			}

			select {
			case aggTx = <-p2p.AggTxChan:
				storage.WriteOpenTx(aggTx)
				logger.Printf("  Received AGGTX: %x", TX)
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				stash := p2p.ReceivedAggTxStash
				if p2p.AggTxAlreadyInStash(stash, TX){
					for _, tx := range stash {
						if tx.Hash() == TX {
							aggTx = tx
							logger.Printf("  FOUND: Request AGGTX: %x", aggTx.Hash())
							break
						}
					}
					break
				}
				if cnt < 2 {
					cnt ++
					goto HERE
				}
				logger.Printf("TIME OUT: Request AGGTX: %x", TX)
				return
			}
			if aggTx.Hash() != TX {
				logger.Printf("Received AggTxHash did not correspond to our request.")
				return
			}
			logger.Printf("Received requested AggTX %x", aggTx.Hash())
			//now delete
			storage.WriteClosedTx(storage.ReadOpenTx(TX))
			storage.DeleteOpenTx(storage.ReadOpenTx(TX))
			logger.Printf("Deleted transaction (%x) from the MemPool. \n",TX)
			for _,fundsTX := range aggTx.(*protocol.AggTx).AggregatedTxSlice {
				if (storage.ReadOpenTx(fundsTX) != nil) {
					storage.WriteClosedTx(storage.ReadOpenTx(fundsTX))
					storage.DeleteOpenTx(storage.ReadOpenTx(fundsTX))
				}
			}
		}
	}
	logger.Printf("Deleted transaction count: %d - New Mempool Size: %d\n",len(contractData)+len(fundsData)+len(configData)+ len(stakeData) + len(aggTxData),storage.GetMemPoolSize())
}

//End code from Kürsat

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
