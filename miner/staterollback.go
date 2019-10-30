package miner

import (
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"sort"
)

func accStateChangeRollback(txSlice []*protocol.AccTx) {
	for _, tx := range txSlice {
		if tx.Header == 0 || tx.Header == 1 || tx.Header == 2 {
			accHash := protocol.SerializeHashContent(tx.PubKey)

			acc, err := storage.GetAccount(accHash)
			if err != nil {
				logger.Fatal("CRITICAL: An account that should have been saved does not exist.")
			}

			delete(storage.State, accHash)

			switch tx.Header {
			case 1:
				delete(storage.RootKeys, accHash)
			case 2:
				storage.RootKeys[accHash] = acc
			}
		}
	}
}

func fundsStateChangeRollback(txSlice []*protocol.FundsTx) {
	//Rollback in reverse order than original state change
	for cnt := len(txSlice) - 1; cnt >= 0; cnt-- {
		tx := txSlice[cnt]

		accSender, _ := storage.GetAccount(tx.From)
		accReceiver, _ := storage.GetAccount(tx.To)

		accSender.TxCnt -= 1
		accSender.Balance += tx.Amount
		accReceiver.Balance -= tx.Amount

		//If new coins were issued, revert
		if rootAcc, _ := storage.GetRootAccount(tx.From); rootAcc != nil {
			rootAcc.Balance -= tx.Amount
			rootAcc.Balance -= tx.Fee
		}
	}
}

//This method does search historic blocks which do not have any transactions inside and now have to be reactivated
// because a transaction validated in this block was aggregated later in a block which now is rolled back. Thus all the
// transactions in this block need ot be reactivated such that they are visible in the chain.
func reactivateHistoricBlockDueToRollback(tx protocol.Transaction)() {
	var fundsTx *protocol.FundsTx
	var aggTx *protocol.AggTx
	var blockHash [32]byte

	switch tx.(type){
	case *protocol.FundsTx:
		fundsTx = tx.(*protocol.FundsTx)
		fundsTx.Aggregated = false
		storage.WriteClosedTx(fundsTx)
		blockHash = fundsTx.Block
	case *protocol.AggTx:
		aggTx = tx.(*protocol.AggTx)
		aggTx.Aggregated = false
		storage.WriteClosedTx(aggTx)
		blockHash = aggTx.Block
	}

	//Search block in bucket & Change aggregated to false.
	block := storage.ReadClosedBlockWithoutTx(blockHash)
	if block == nil {
		//Block is in closed blocks but not aggregated --> only change transactions to aggregated = false what is done above.
		return
	}

	//Search all transactions which were validated in the "empty" block. This may be very time consuming.
	for _, tx := range storage.ReadAllClosedFundsAndAggTransactions() {
		switch tx.(type){
		case *protocol.FundsTx:
			FTX := tx.(*protocol.FundsTx)
			if FTX.Block == blockHash {
				//Reactivate this transaction --> it is still closed but it has to be visible in the block and chain again.
				block.FundsTxData = append(block.FundsTxData, tx.Hash())
				storage.WriteClosedTx(FTX)
			}
		case *protocol.AggTx:
			ATX := tx.(*protocol.AggTx)
			if ATX.Block == blockHash {
				//Reactivate this transaction
				block.AggTxData = append(block.AggTxData, tx.Hash())
				storage.WriteClosedTx(ATX)
			}
		}
	}


	sort.Sort(ByHash(block.AggTxData))

	//Write block back to bucket closed blocks with transactions.
	//Todo uncomment
	//block.Aggregated = false
	storage.WriteClosedBlock(block)
	storage.DeleteClosedBlockWithoutTx(blockHash)
	//TODO uncomment
	//logger.Printf("UPDATE: Rollback Write (%x) into closedBlockBucket as (%x)", block.HashWithoutTx[0:8], block.Hash[0:8])
}

type ByHash [][32]byte
func (a ByHash) Len() int           { return len(a) }
func (a ByHash) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHash) Less(i, j int) bool { return string(a[i][0:32]) < string(a[j][0:32]) }

func aggregatedStateRollback(txSlice []*protocol.AggTx, blockHash [32]byte, minerHash [32]byte) {
	//Rollback in reverse order than original state change
	var fundsTxSlice []*protocol.FundsTx

	for cnt := len(txSlice) - 1; cnt >= 0; cnt-- {
		tx := txSlice[cnt]

		//Adding all Aggregated FundsTx in reverse order.
		for _, txHash := range tx.AggregatedTxSlice {
			trx := storage.ReadClosedTx(txHash)
			switch trx.(type) {
			case *protocol.FundsTx:
				//Only rollback FundsTx which are validated in the current block.
				if trx.(*protocol.FundsTx).Block == blockHash {
					fundsTxSlice = append([]*protocol.FundsTx{trx.(*protocol.FundsTx)},fundsTxSlice...)
				} else {
					//Transaction is not validated in the current block --> Need to be reactivated in the historic block.
					reactivateHistoricBlockDueToRollback(trx)
				}
			case *protocol.AggTx:
				//AggTx can only be historic at this point and therefore need to be reactivated in the historic block.
				reactivateHistoricBlockDueToRollback(trx)
			}
		}

		//do normal rollback for fundsTx And Fees
		sort.Sort(ByTxCount(fundsTxSlice))
		fundsStateChangeRollback(fundsTxSlice)
		collectTxFeesRollback(nil, fundsTxSlice, nil, nil, minerHash)
		fundsTxSlice = fundsTxSlice[:0]
	}
}

func configStateChangeRollback(txSlice []*protocol.ConfigTx, blockHash [32]byte) {
	if len(txSlice) == 0 {
		return
	}

	//Only rollback if the config changes lead to a parameterChange
	//there might be the case that the client is not running the latest version, it's still confirming
	//the transaction but does not understand the ID and thus is not changing the state
	if parameterSlice[len(parameterSlice)-1].BlockHash != blockHash {
		return
	}

	//remove the latest entry in the parameters slice$
	parameterSlice = parameterSlice[:len(parameterSlice)-1]
	activeParameters = &parameterSlice[len(parameterSlice)-1]
	logger.Printf("Config parameters rolled back. New configuration: %v", *activeParameters)
}

func stakeStateChangeRollback(txSlice []*protocol.StakeTx) {
	//Rollback in reverse order than original state change
	for cnt := len(txSlice) - 1; cnt >= 0; cnt-- {
		tx := txSlice[cnt]

		accSender, _ := storage.GetAccount(tx.Account)
		//Rolling back stakingBlockHeight not needed
		accSender.IsStaking = !accSender.IsStaking
	}
}

func collectTxFeesRollback(accTx []*protocol.AccTx, fundsTx []*protocol.FundsTx, configTx []*protocol.ConfigTx, stakeTx []*protocol.StakeTx, minerHash [32]byte) {
	minerAcc, _ := storage.GetAccount(minerHash)

	//Subtract fees from sender (check if that is allowed has already been done in the block validation)
	for _, tx := range accTx {
		//Money was created out of thin air, no need to write back
		minerAcc.Balance -= tx.Fee
	}

	for _, tx := range fundsTx {
		minerAcc.Balance -= tx.Fee

		senderAcc, _ := storage.GetAccount(tx.From)
		senderAcc.Balance += tx.Fee
	}

	for _, tx := range configTx {
		//Money was created out of thin air, no need to write back
		minerAcc.Balance -= tx.Fee
	}

	for _, tx := range stakeTx {
		minerAcc.Balance -= tx.Fee

		senderAcc, _ := storage.GetAccount(tx.Account)
		senderAcc.Balance += tx.Fee
	}
}

func collectBlockRewardRollback(reward uint64, minerHash [32]byte) {
	minerAcc, _ := storage.GetAccount(minerHash)
	minerAcc.Balance -= reward
}

func collectSlashRewardRollback(reward uint64, block *protocol.Block) {
	//TODO add to conditional: || block.ConflictingBlockHashWithoutTx1 != [32]byte{} || block.ConflictingBlockHashWithoutTx2 != [32]byte{}
	if block.SlashedAddress != [32]byte{} || block.ConflictingBlockHash1 != [32]byte{} || block.ConflictingBlockHash2 != [32]byte{} {
		minerAcc, _ := storage.GetAccount(block.Beneficiary)
		slashedAcc, _ := storage.GetAccount(block.SlashedAddress)

		minerAcc.Balance -= reward
		slashedAcc.Balance += activeParameters.Staking_minimum
		slashedAcc.IsStaking = true
	}
}
