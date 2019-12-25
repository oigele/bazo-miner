package storage

import (
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/boltdb/bolt"
	"sort"
)

//Always return nil if requested hash is not in the storage. This return value is then checked against by the caller
func ReadOpenBlock(hash [32]byte) (block *protocol.Block) {

	var encodedBlock []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("openblocks"))
		encodedBlock = b.Get(hash[:])
		return nil
	})

	if encodedBlock == nil {
		return nil
	}

	return block.Decode(encodedBlock)
}

func ReadClosedBlock(hash [32]byte) (block *protocol.Block) {

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblocks"))
		encodedBlock := b.Get(hash[:])
		block = block.Decode(encodedBlock)
		return nil
	})

	if block == nil {
		return nil
	}

	return block
}

func ReadOpenEpochBlock(hash [32]byte) (epochBlock *protocol.EpochBlock) {
	var encodedEpochBlock []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(OPENEPOCHBLOCK_BUCKET))
		encodedEpochBlock = b.Get(hash[:])
		return nil
	})

	if encodedEpochBlock == nil {
		return nil
	}

	return epochBlock.Decode(encodedEpochBlock)
}

func ReadClosedEpochBlock(hash [32]byte) (epochBlock *protocol.EpochBlock) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		encodedBlock := b.Get(hash[:])
		epochBlock = epochBlock.Decode(encodedBlock)
		return nil
	})

	if epochBlock == nil {
		return nil
	}

	return epochBlock
}

//This function does read all blocks without transactions inside.
func ReadClosedBlockWithoutTx(hash [32]byte) (block *protocol.Block) {

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedblockswithouttx"))
		encodedBlock := b.Get(hash[:])
		block = block.Decode(encodedBlock)
		return nil
	})

	if block == nil {
		return nil
	}

	return block
}

func ReadLastClosedBlock() (block *protocol.Block) {

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lastclosedblock"))
		cb := b.Cursor()
		_, encodedBlock := cb.First()
		block = block.Decode(encodedBlock)
		return nil
	})

	if block == nil {
		return nil
	}

	return block
}

func ReadAllClosedBlocksWithTransactions() (allClosedBlocks []*protocol.Block) {

	//This does return all blocks which are either in closedblocks or closedblockswithouttx bucket of the Database.
	//They are not ordered at teh request, but this does actually not matter. Because it will be ordered below
	block := ReadLastClosedBlock()
	if  block != nil {
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("closedblocks"))
			b.ForEach(func(k, v []byte) error {
				if v != nil {
					encodedBlock := v
					block = block.Decode(encodedBlock)
					allClosedBlocks = append(allClosedBlocks, block)
				}
				return nil
			})
			return nil
		})
	}

	//blocks are sorted here.
	sort.Sort(ByHeight(allClosedBlocks))

	return allClosedBlocks
}

func ReadAllClosedFundsAndAggTransactions() (allClosedTransactions []protocol.Transaction) {

	var fundsTx protocol.FundsTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedfunds"))
		b.ForEach(func(k, v []byte) error {
			if v != nil {
				encodedFundsTx := v
				allClosedTransactions = append(allClosedTransactions, fundsTx.Decode(encodedFundsTx))
			}
			return nil
		})
		return nil
	})

	var aggTx protocol.AggTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedaggregations"))
		b.ForEach(func(k, v []byte) error {
			if v != nil {
				encodedAggTx := v
				if encodedAggTx != nil{
					allClosedTransactions = append(allClosedTransactions, aggTx.Decode(encodedAggTx))
				}
			}
			return nil
		})
		return nil
	})

	return allClosedTransactions
}

//This method does read all blocks in closedBlocks & closedblockswithouttx.
func ReadAllClosedBlocks() (allClosedBlocks []*protocol.Block) {

	//This does return all blocks which are either in closedblocks or closedblockswithouttx bucket of the Database.
	//They are not ordered at teh request, but this does actually not matter. Because it will be ordered below
	block := ReadLastClosedBlock()
	if  block != nil {
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("closedblocks"))
			b.ForEach(func(k, v []byte) error {
				if v != nil {
					encodedBlock := v
					block = block.Decode(encodedBlock)
					allClosedBlocks = append(allClosedBlocks, block)
				}
				return nil
			})

			b = tx.Bucket([]byte("closedblockswithouttx"))
			b.ForEach(func(k, v []byte) error {
				if v != nil {
					encodedBlock := v
					block = block.Decode(encodedBlock)
					allClosedBlocks = append(allClosedBlocks, block)
				}
				return nil
			})

			return nil
		})
	}

	//blocks are sorted here.
	sort.Sort(ByHeight(allClosedBlocks))

	return allClosedBlocks
}

//The three functions and the type below are used to order the gathered closed blocks from below according to
//their block height.
type ByHeight []*protocol.Block
func (a ByHeight) Len() int           { return len(a) }
func (a ByHeight) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHeight) Less(i, j int) bool { return a[i].Height < a[j].Height }


func ReadReceivedBlockStash() (receivedBlocks []*protocol.Block){
	return ReceivedBlockStash
}

func ReadOpenTx(hash [32]byte) (transaction protocol.Transaction) {
	openTxMutex.Lock()
	defer openTxMutex.Unlock()
	return txMemPool[hash]
}

func ReadTxcntToTx(txCnt uint32) (transactions [][32]byte) {
	txcntToTxMapMutex.Lock()
	defer txcntToTxMapMutex.Unlock()
	return TxcntToTxMap[txCnt]
}

func ReadFundsTxBeforeAggregation() ([]*protocol.FundsTx) {
	openFundsTxBeforeAggregationMutex.Lock()
	defer openFundsTxBeforeAggregationMutex.Unlock()
	return FundsTxBeforeAggregation
}

func ReadAllBootstrapReceivedTransactions() (allOpenTxs []protocol.Transaction) {
	openTxMutex.Lock()
	defer openTxMutex.Unlock()

	for key := range bootstrapReceivedMemPool {
		allOpenTxs = append(allOpenTxs, txMemPool[key])
	}
	return
}

func ReadINVALIDOpenTx(hash [32]byte) (transaction protocol.Transaction) {
	openINVALIDTxMutex.Lock()
	defer openINVALIDTxMutex.Unlock()
	return txINVALIDMemPool[hash]
}

func ReadAllINVALIDOpenTx() (allOpenInvalidTxs []protocol.Transaction) {

	openINVALIDTxMutex.Lock()
	defer openINVALIDTxMutex.Unlock()
	for key := range txINVALIDMemPool {
		allOpenInvalidTxs = append(allOpenInvalidTxs, txINVALIDMemPool[key])
	}

	return allOpenInvalidTxs
}

func ReadAccount(pubKey [64]byte) (acc *protocol.Account, err error) {
	if acc = State[protocol.SerializeHashContent(pubKey)]; acc != nil {
		return acc, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Acc (%x) not in the state.", pubKey[0:8]))
	}
}

func ReadGenesis() (genesis *protocol.Genesis, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(GENESIS_BUCKET))
		encoded := b.Get([]byte("genesis"))
		genesis = genesis.Decode(encoded)
		return nil
	})
	return genesis, err
}

func ReadFirstEpochBlock() (firstEpochBlock *protocol.EpochBlock, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		encoded := b.Get([]byte("firstepochblock"))
		firstEpochBlock = firstEpochBlock.Decode(encoded)
		return nil
	})
	return firstEpochBlock, err
}

func ReadFirstShardBlock() (firstShardBlock *protocol.ShardBlock, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		encoded := b.Get([]byte("firstshardblock"))
		firstShardBlock = firstShardBlock.Decode(encoded)
		return nil
	})
	return firstShardBlock, err
}
func ReadLastClosedEpochBlock() (epochBlock *protocol.EpochBlock) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LASTCLOSEDEPOCHBLOCK_BUCKET))
		cb := b.Cursor()
		_, encodedBlock := cb.First()
		epochBlock = epochBlock.Decode(encodedBlock)
		return nil
	})

	if epochBlock == nil {
		return nil
	}

	return epochBlock
}
func ReadLastClosedShardBlock() (shardBlock *protocol.ShardBlock) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(LASTCLOSEDSHARDBLOCK_BUCKET))
		cb := b.Cursor()
		_, encodedBlock := cb.First()
		shardBlock = shardBlock.Decode(encodedBlock)
		return nil
	})

	if shardBlock == nil {
		return nil
	}

	return shardBlock
}

func GetMemPoolSize() int {
	memPoolMutex.Lock()
	defer memPoolMutex.Unlock()
	return len(txMemPool)
}

//Needed for the miner to prepare a new block
func ReadAllOpenTxs() (allOpenTxs []protocol.Transaction) {
	openTxMutex.Lock()
	defer openTxMutex.Unlock()

	for key := range txMemPool {
		allOpenTxs = append(allOpenTxs, txMemPool[key])
	}
	return
}

//Personally I like it better to test (which tx type it is) here, and get returned the interface. Simplifies the code
func ReadClosedTx(hash [32]byte) (transaction protocol.Transaction) {
	var encodedTx []byte
	var fundstx *protocol.FundsTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedfunds"))
		encodedTx = b.Get(hash[:])
		return nil
	})
	if encodedTx != nil {
		return fundstx.Decode(encodedTx)
	}

	var acctx *protocol.AccTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedaccs"))
		encodedTx = b.Get(hash[:])
		return nil
	})
	if encodedTx != nil {
		return acctx.Decode(encodedTx)
	}

	var configtx *protocol.ConfigTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedconfigs"))
		encodedTx = b.Get(hash[:])
		return nil
	})
	if encodedTx != nil {
		return configtx.Decode(encodedTx)
	}

	var staketx *protocol.StakeTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedstakes"))
		encodedTx = b.Get(hash[:])
		return nil
	})
	if encodedTx != nil {
		return staketx.Decode(encodedTx)
	}

	var aggTx *protocol.AggTx
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("closedaggregations"))
		encodedTx = b.Get(hash[:])
		return nil
	})
	if encodedTx != nil {
		return aggTx.Decode(encodedTx)
	}

	return nil
}

func ReadStateTransitionFromOwnStash(height int) *protocol.StateTransition {
	for _, st := range OwnStateTransitionStash {
		if(int(st.Height) == height){
			return st
		}
	}

	return nil
}

func ReadBlockTransitionFromOwnStash(height int) *protocol.BlockTransition {
	for _, bt := range OwnBlockTransitionStash {
		if bt.Height == height {
			return bt
		}
	}
	return nil
}