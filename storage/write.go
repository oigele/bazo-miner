package storage

import (
	"github.com/boltdb/bolt"
	"github.com/oigele/bazo-miner/protocol"
)

func WriteOpenBlock(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("openblocks"))
		err := b.Put(block.Hash[:], block.Encode())
		return err
	})

	return err
}

func WriteClosedBlock(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblocks"))
		err := b.Put(block.Hash[:], block.Encode())
		return err
	})

	return err
}

func WriteClosedEpochBlock(epochBlock *protocol.EpochBlock) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		return b.Put(epochBlock.Hash[:], epochBlock.Encode())
	})
}

func WriteFirstEpochBlock(epochBlock *protocol.EpochBlock) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		return b.Put([]byte("firstepochblock"), epochBlock.Encode())
	})
}

func WriteLastClosedEpochBlock(epochBlock *protocol.EpochBlock) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LASTCLOSEDEPOCHBLOCK_BUCKET))
		return b.Put(epochBlock.Hash[:], epochBlock.Encode())
	})
}

func WriteGenesis(genesis *protocol.Genesis) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(GENESIS_BUCKET))
		return b.Put([]byte("genesis"), genesis.Encode())
	})
}

/* TODO UNCOMMENT
func WriteClosedBlockWithoutTx(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblockswithouttx"))
		err := b.Put(block.HashWithoutTx[:], block.Encode())
		return err
	})

	return err
}
*/
func WriteLastClosedBlock(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		err := b.Put(block.Hash[:], block.Encode())
		return err
	})

	return err
}

//Changing the "tx" shortcut here and using "transaction" to distinguish between bolt's transactions
//write open tx doesnt allow for a tx to be added twice
func WriteOpenTx(transaction protocol.Transaction) {
	openTxMutex.Lock()
	txMemPool[transaction.Hash()] = transaction

	switch transaction.(type) {
	case *protocol.FundsTx:
		//This is interesting if missing transactions have to be queried based on their transaction count
		WriteTxcntToTx(transaction.(*protocol.FundsTx))
	}

	openTxMutex.Unlock()
}


func WriteOpenTxHashToDelete(hash [32]byte) {
	openTxToDeleteMutex.Lock()
	defer openTxToDeleteMutex.Unlock()
	openTxToDeleteMempool[hash] = true

}

func ResetOpenTxHashToDeleteMempool() {
	openTxToDeleteMutex.Lock()
	defer openTxToDeleteMutex.Unlock()
	openTxToDeleteMempool = make(map[[32]byte]bool)
}

func WriteTxcntToTx(transaction *protocol.FundsTx) {
	txcntToTxMapMutex.Lock()
	TxcntToTxMap[transaction.TxCnt] = append(TxcntToTxMap[transaction.TxCnt], transaction.Hash())
	txcntToTxMapMutex.Unlock()
}

func WriteFundsTxBeforeAggregation(transaction *protocol.FundsTx) {
	openFundsTxBeforeAggregationMutex.Lock()
	FundsTxBeforeAggregation = append(FundsTxBeforeAggregation, transaction)
	openFundsTxBeforeAggregationMutex.Unlock()
}

func WriteDataTxBeforeAggregation(transaction *protocol.DataTx){
	openDataTxBeforeAggregationMutex.Lock()
	defer openDataTxBeforeAggregationMutex.Unlock()
	DataTxBeforeAggregation = append(DataTxBeforeAggregation, transaction)
}

func WriteBootstrapTxReceived(transaction protocol.Transaction) {
	bootstrapReceivedMemPool[transaction.Hash()] = transaction
}

func WriteINVALIDOpenTx(transaction protocol.Transaction) {
	openINVALIDTxMutex.Lock()
	txINVALIDMemPool[transaction.Hash()] = transaction
	openINVALIDTxMutex.Unlock()
}

func WriteToReceivedStash(block *protocol.Block) {
	ReceivedBlockStashMutex.Lock()
	//Only write it to stash if it is not in there already.
	if !BlockAlreadyInStash(ReceivedBlockStash, block.Hash) {
		ReceivedBlockStash = append(ReceivedBlockStash, block)

		//When length of stash is > 100 --> Remove first added Block
		if len(ReceivedBlockStash) > 100 {
			ReceivedBlockStash = append(ReceivedBlockStash[:0], ReceivedBlockStash[1:]...)
		}
	}
	ReceivedBlockStashMutex.Unlock()
}

func BlockAlreadyInStash(slice []*protocol.Block, newBlockHash [32]byte) bool {
	for _, blockInStash := range slice {
		if blockInStash.Hash == newBlockHash {
			return true
		}
	}
	return false
}

func WriteClosedFundsTxFromAggTxSlice(transactions []protocol.FundsTx) (err error) {
	bucket := "closedfunds"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range transactions {
			hash := transaction.Hash()
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})
	return err
}

//In this method, we can detect whether the transaction included in the block is already in closed storage.
//If it is already there, there are 2 options. Either the committee or the shard was malicious
//If bespoke transaction was in the transaction assignment, the committee leader was malicious
//If bespoke transaction was not in the transaction assignment, the shard was malicious
//To make the code more efficient and performant, the check of who is actually malicious will be conducted at a different part of the code
func WriteAllClosedTxAndReturnAlreadyClosedTxHashes(accTxs []*protocol.AccTx, stakeTxs []*protocol.StakeTx, committeeTxs []*protocol.CommitteeTx, fundsTxs []*protocol.FundsTx, aggTxs []*protocol.AggTx, dataTxs []*protocol.DataTx, aggDataTxs []*protocol.AggDataTx) (alreadyIncludedTxHashes [][32]byte, err error) {
	bucket := "closedaccs"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range accTxs {
			hash := transaction.Hash()
			var accTx *protocol.AccTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}

			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closedstakes"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range stakeTxs {
			hash := transaction.Hash()
			var accTx *protocol.StakeTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closedcommittees"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range committeeTxs {
			hash := transaction.Hash()
			var accTx *protocol.CommitteeTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closedfunds"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range fundsTxs {
			hash := transaction.Hash()
			var accTx *protocol.FundsTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closedaggregations"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range aggTxs {
			hash := transaction.Hash()
			var accTx *protocol.AggTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closeddata"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range dataTxs {
			hash := transaction.Hash()
			var accTx *protocol.DataTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	bucket = "closedaggdata"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range aggDataTxs {
			hash := transaction.Hash()
			var accTx *protocol.AggDataTx
			encodedTx := b.Get(hash[:])
			//this means that an already closed Tx was included in the block
			if hash == accTx.Decode(encodedTx).Hash() {
				alreadyIncludedTxHashes = append(alreadyIncludedTxHashes, hash)
			}
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize / nrClosedTransactions
		}
		return err
	})

	return alreadyIncludedTxHashes, err
}

func WriteClosedTx(transaction protocol.Transaction) (err error) {

	var bucket string
	switch transaction.(type) {
	case *protocol.FundsTx:
		bucket = "closedfunds"
	case *protocol.AccTx:
		bucket = "closedaccs"
	case *protocol.ConfigTx:
		bucket = "closedconfigs"
	case *protocol.StakeTx:
		bucket = "closedstakes"
	case *protocol.AggTx:
		bucket = "closedaggregations"
	case *protocol.DataTx:
		bucket = "closeddata"
	case *protocol.AggDataTx:
		bucket = "closedaggdata"
	}

	hash := transaction.Hash()
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put(hash[:], transaction.Encode())
		return err
	})

	nrClosedTransactions = nrClosedTransactions + 1
	totalTransactionSize = totalTransactionSize + float32(transaction.Size())
	averageTxSize = totalTransactionSize / nrClosedTransactions
	return err
}

func WriteToOwnStateTransitionkStash(st *protocol.StateTransition) {
	OwnStateTransitionStash = append(OwnStateTransitionStash, st)

	if len(OwnStateTransitionStash) > 20 {
		OwnStateTransitionStash = append(OwnStateTransitionStash[:0], OwnStateTransitionStash[1:]...)
	}
}

func WriteDataSummary(ds *protocol.DataSummary) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("datasummary"))
		b.Put(ds.Address[:], ds.Encode())
		return err
	})
	return err
}
