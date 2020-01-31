package storage

import (
	"errors"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/boltdb/bolt"
)

//There exist open/closed buckets and closed tx buckets for all types (open txs are in volatile storage)
func DeleteOpenBlock(hash [32]byte) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("openblocks"))
		err := b.Delete(hash[:])
		return err
	})
}

func DeleteClosedBlock(hash [32]byte) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblocks"))
		err := b.Delete(hash[:])
		return err
	})
}

func DeleteClosedEpochBlock(hash [32]byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		return b.Delete(hash[:])
	})
}

func DeleteOpenEpochBlock(hash [32]byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(OPENEPOCHBLOCK_BUCKET))
		return b.Delete(hash[:])
	})
}

func DeleteClosedBlockWithoutTx(hash [32]byte) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblockswithouttx"))
		err := b.Delete(hash[:])
		return err
	})
}

func DeleteLastClosedBlock(hash [32]byte) {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		err := b.Delete(hash[:])
		return err
	})
}

func DeleteAllLastClosedBlock() {
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
}

func DeleteAllLastClosedEpochBlock() error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LASTCLOSEDEPOCHBLOCK_BUCKET))
		return b.ForEach(func(k, v []byte) error {
			return b.Delete(k)
		})
	})
}

func DeleteOpenTx(transaction protocol.Transaction) {
	openTxMutex.Lock()
	delete(txMemPool, transaction.Hash())
	openTxMutex.Unlock()
}

func DeleteAllOpenTx(accTxs []*protocol.AccTx, stakeTxs []*protocol.StakeTx, fundsTxs []*protocol.FundsTx, aggTxs []*protocol.AggTx, dataTxs []*protocol.DataTx, aggDataTxs []*protocol.AggDataTx) error {
	openTxMutex.Lock()
	defer openTxMutex.Unlock()

	//Before deleting any element, look if it's in the local memory pool in the first place. If it's not, then throw an error.
	//Because this would mean that the Shard intentionally included a bogus transaction. Since the committee does not verify these,
	//A test here is the most effective that we can do.
	for _, transaction := range accTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	for _, transaction := range stakeTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	for _, transaction := range fundsTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	for _, transaction := range dataTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	for _, transaction := range aggTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	for _, transaction := range aggDataTxs {
		txHash := transaction.Hash()
		if _, exists := txMemPool[txHash]; !exists {
			return errors.New("The Shard has put a transaction into its block that should not be in there")
		} else {
			delete(txMemPool, txHash)
		}
	}
	return nil
}


func DeleteINVALIDOpenTx(transaction protocol.Transaction) {
	openINVALIDTxMutex.Lock()
	delete(txINVALIDMemPool, transaction.Hash())
	openINVALIDTxMutex.Unlock()
}


func DeleteAllFundsTxBeforeAggregation(){
	FundsTxBeforeAggregation = nil
}

func DeleteAllDataTxBeforeAggregation() {
	DataTxBeforeAggregation = nil
}

func DeleteClosedTx(transaction protocol.Transaction) {
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
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Delete(hash[:])
		return err
	})

	nrClosedTransactions = nrClosedTransactions - 1
	totalTransactionSize = totalTransactionSize - float32(transaction.Size())
	averageTxSize = totalTransactionSize/nrClosedTransactions
}

func DeleteBootstrapReceivedMempool() {
	//Delete in-memory storage
	for key := range txMemPool {
		delete(bootstrapReceivedMemPool, key)
	}
}



func DeleteAll() {
	//Delete in-memory storage
	for key := range txMemPool {
		delete(txMemPool, key)
	}

	//Delete disk-based storage
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("openblocks"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblocks"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblockswithouttx"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedfunds"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedaccs"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedconfigs"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedstakes"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		b.ForEach(func(k, v []byte) error {
			b.Delete(k)
			return nil
		})
		return nil
	})
}
