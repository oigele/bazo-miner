package storage

import (
	"github.com/oigele/bazo-miner/protocol"
	"github.com/boltdb/bolt"
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
			averageTxSize = totalTransactionSize/nrClosedTransactions
		}
		return err
	})
	return err
}

func WriteAllClosedTx(accTxs []*protocol.AccTx, stakeTxs []*protocol.StakeTx, fundsTxs []*protocol.FundsTx) (err error) {
	bucket := "closedaccs"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range accTxs {
			hash := transaction.Hash()
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize/nrClosedTransactions
		}
		return err
	})

	bucket = "closedstakes"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range stakeTxs {
			hash := transaction.Hash()
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize/nrClosedTransactions
		}
		return err
	})

	bucket = "closedfunds"
	err = db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucket))
		for _, transaction := range fundsTxs {
			hash := transaction.Hash()
			err = b.Put(hash[:], transaction.Encode())
			if err != nil {
				logger.Printf("We GOT AN ERROR")
			}
			nrClosedTransactions = nrClosedTransactions + 1
			totalTransactionSize = totalTransactionSize + float32(transaction.Size())
			averageTxSize = totalTransactionSize/nrClosedTransactions
		}
		return err
	})


	return err
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
	}

	hash := transaction.Hash()
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put(hash[:], transaction.Encode())
		return err
	})

	nrClosedTransactions = nrClosedTransactions + 1
	totalTransactionSize = totalTransactionSize + float32(transaction.Size())
	averageTxSize = totalTransactionSize/nrClosedTransactions
	return err
}

func WriteToOwnStateTransitionkStash(st *protocol.StateTransition) {
	OwnStateTransitionStash = append(OwnStateTransitionStash,st)

	if(len(OwnStateTransitionStash) > 20){
		OwnStateTransitionStash = append(OwnStateTransitionStash[:0], OwnStateTransitionStash[1:]...)
	}
}