package storage

import (
	"github.com/boltdb/bolt"
	"github.com/oigele/bazo-miner/protocol"
)

/* TODO UNCOMMENT
func UpdateBlocksToBlocksWithoutTx(block *protocol.Block) (err error){

	if BlockReadyToAggregate(block) {
		block.Aggregated = true
		WriteClosedBlockWithoutTx(block)
		DeleteClosedBlock(block.Hash)
		logger.Printf("UPDATE: Write (%x) into closedBlockWithoutTransactions-Bucket as (%x)", block.Hash[0:8], block.HashWithoutTx[0:8])
		return err
	}
	return
}
*/
func BlockReadyToAggregate(block *protocol.Block) bool {

	// If Block contains no transactions, it can be viewed as aggregated and moved to the according bucket.
	if (block.NrAggTx == 0) && (block.NrStakeTx == 0) && (block.NrFundsTx == 0) && (block.NrAccTx == 0) && (block.NrConfigTx == 0) {
		return true
	}

	//If block has AccTx, StakeTx or ConfigTx included, it will never be aggregated.
	if (block.NrStakeTx > 0) && (block.NrAccTx > 0) && (block.NrConfigTx > 0) {
		return false
	}

	//Check if all FundsTransactions are aggregated. If not, block cannot be moved to the empty blocks bucket.
	for _, txHash := range block.FundsTxData {
		tx := ReadClosedTx(txHash)

		if tx == nil {
			return false
		}

		if tx.(*protocol.FundsTx).Aggregated == false {
			return false
		}
	}

	for _, txHash := range block.AggTxData {
		tx := ReadClosedTx(txHash)

		if tx == nil {
			return false
		}

		if tx.(*protocol.AggTx).Aggregated == false {
			return false
		}
	}

	//All TX are aggregated and can be emptied.
	block.FundsTxData = nil
	block.NrFundsTx = 0
	block.AggTxData = nil
	block.NrAggTx = 0

	return true
}

func UpdateDataSummary(dataTxs []*protocol.DataTx) (err error){
	var encodedOldDataSummary []byte
	var oldDataSummary *protocol.DataSummary
	var newDataSummary *protocol.DataSummary
	var sender [32]byte
	var dataTxSliceOfSender []*protocol.DataTx
	var updateMap = make(map[[32]byte][]*protocol.DataTx)

	//no dataTxs to update, return
	if !(len(dataTxs) > 0) {
		return nil
	}

	for _,dataTx := range dataTxs {
		updateMap[dataTx.From] = append(updateMap[dataTx.From], dataTx)
	}

	//iterate through each sender in the map. For each, update the database.
	for sender, dataTxSliceOfSender = range updateMap {
		//check if there already exists an entry for our sender
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("datasummary"))
			encodedOldDataSummary = b.Get(sender[:])
			return nil
		})
		//write a new summary for the sender and abort
		if encodedOldDataSummary == nil {
			newDataSummary = protocol.NewDataSummary(sender)
			//aggregate the new summary
			for _, dataTx := range dataTxSliceOfSender {
				if dataTx.Data != nil {
					newDataSummary.Data = append(newDataSummary.Data, dataTx.Data)
				}
			}
			//if there is nothing to write, then dont write it
			if newDataSummary.Data == nil {
				logger.Printf("None of the transactions had data. Not writing a new data summary to database")
				continue
			}
			err = WriteDataSummary(newDataSummary); if err != nil {
				logger.Printf("Got an error when writing data Summary")
				return err
			}
			//there is already a summary for the sender in the database. Need to update it
		} else {
			oldDataSummary = oldDataSummary.Decode(encodedOldDataSummary)
			for _, dataTx := range dataTxSliceOfSender {
				if dataTx.Data != nil {
					oldDataSummary.Data = append(oldDataSummary.Data, dataTx.Data)
				}
			}
			//newDataSummary now contains all the updates, so activate it
			newDataSummary = oldDataSummary
			//delete the old entry and write the new entry to database
			err = db.Update(func(tx *bolt.Tx) error {
				var err error
				b := tx.Bucket([]byte("datasummary"))
				err = b.Delete(sender[:])
				err = b.Put(sender[:], newDataSummary.Encode())
				return err
			})
			oldDataSummary = nil
			newDataSummary = nil
		}
		encodedOldDataSummary = nil
		oldDataSummary = nil
		newDataSummary = nil
	}
	return err
}
