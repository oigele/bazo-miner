package storage

import (
	"github.com/bazo-blockchain/bazo-miner/protocol"
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

func WriteClosedBlockWithoutTx(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblockswithouttx"))
		err := b.Put(block.HashWithoutTx[:], block.Encode())
		return err
	})

	return err
}

func WriteLastClosedBlock(block *protocol.Block) (err error) {

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		err := b.Put(block.Hash[:], block.Encode())
		return err
	})

	return err
}

//Changing the "tx" shortcut here and using "transaction" to distinguish between bolt's transactions
func WriteOpenTx(transaction protocol.Transaction) {
	openTxMutex.Lock()
	txMemPool[transaction.Hash()] = transaction

	switch transaction.(type) {
	case *protocol.FundsTx:
		WriteTxcntToTx(transaction.(*protocol.FundsTx))
	}

	openTxMutex.Unlock()
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
	if !blockAlreadyInStash(ReceivedBlockStash, block.Hash) {
		ReceivedBlockStash = append(ReceivedBlockStash, block)

		//When length of stash is > 100 --> Remove first added Block
		if len(ReceivedBlockStash) > 100 {
			ReceivedBlockStash = append(ReceivedBlockStash[:0], ReceivedBlockStash[1:]...)
		}
	}
	ReceivedBlockStashMutex.Unlock()
}

func blockAlreadyInStash(slice []*protocol.Block, newBlockHash [32]byte) bool {
	for _, blockInStash := range slice {
		if blockInStash.Hash == newBlockHash {
			return true
		}
	}
	return false
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