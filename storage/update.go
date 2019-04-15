package storage

import (
	"github.com/bazo-blockchain/bazo-miner/protocol"
)


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
