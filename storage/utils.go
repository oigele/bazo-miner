package storage

import (
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/protocol"
	"io"
	"log"
	"os"
	"time"
)

func InitLogger() *log.Logger {

	//Create a Log-file (Logger.Miner.log) and write all logger.printf(...) Statements into it.

	//use this two lines, if all miners should have distinct names for their log files. 
	time.Now().Format("030405")
	filename := "LoggerMiner" + time.Now().Format("150405") + ".log"

	//use this line when all miners should have the same log file name.
	//filename := "LoggerMiner.log"
	LogFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	wrt := io.MultiWriter(os.Stdout, LogFile)
	log.SetOutput(wrt)
	return log.New(wrt, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

//Needed by miner and p2p package
func GetAccount(hash [32]byte) (acc *protocol.Account, err error) {
	if acc = State[hash]; acc != nil {
		return acc, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Acc (%x) not in the state.", hash[0:8]))
	}
}

func GetRootAccount(hash [32]byte) (acc *protocol.Account, err error) {
	if IsRootKey(hash) {
		acc, err = GetAccount(hash)
		return acc, err
	}

	return nil, err
}

func IsRootKey(hash [32]byte) bool {
	_, exists := RootKeys[hash]
	return exists
}

//Get all pubKeys involved in AccTx, FundsTx of a given block
func GetTxPubKeys(block *protocol.Block) (txPubKeys [][32]byte) {
	txPubKeys = GetAccTxPubKeys(block.AccTxData)
	txPubKeys = append(txPubKeys, GetFundsTxPubKeys(block.FundsTxData)...)

	return txPubKeys
}

//Get all pubKey involved in AccTx
func GetAccTxPubKeys(accTxData [][32]byte) (accTxPubKeys [][32]byte) {
	for _, txHash := range accTxData {
		var tx protocol.Transaction
		var accTx *protocol.AccTx

		tx = ReadClosedTx(txHash)
		if tx == nil {
			tx = ReadOpenTx(txHash)
		}

		accTx = tx.(*protocol.AccTx)
		accTxPubKeys = append(accTxPubKeys, accTx.Issuer)
		accTxPubKeys = append(accTxPubKeys, protocol.SerializeHashContent(accTx.PubKey))
	}

	return accTxPubKeys
}
func GetRelativeState(statePrev map[[32]byte]protocol.Account, stateNow map[[32]byte]*protocol.Account) (stateRel map[[32]byte]*protocol.RelativeAccount) {
	var stateRelative = make(map[[32]byte]*protocol.RelativeAccount)

	for know, _ := range stateNow {
		//In case account was newly created during block validation
		if _, ok := statePrev[know]; !ok {
			accNow := stateNow[know]
			accNewRel := protocol.NewRelativeAccount(stateNow[know].Address, [32]byte{}, int64(accNow.Balance), accNow.IsStaking, accNow.CommitmentKey, accNow.Contract, accNow.ContractVariables)
			accNewRel.TxCnt = int32(accNow.TxCnt)
			accNewRel.StakingBlockHeight = int32(accNow.StakingBlockHeight)
			stateRelative[know] = &accNewRel
		} else {
			//Get account as in the version before block validation
			accPrev := statePrev[know]
			accNew := stateNow[know]

			//account with relative adjustments of the fields, will be  applied by the other shards
			accTransition := protocol.NewRelativeAccount(stateNow[know].Address, [32]byte{}, int64(accNew.Balance-accPrev.Balance), accNew.IsStaking, accNew.CommitmentKey, accNew.Contract, accNew.ContractVariables)
			accTransition.TxCnt = int32(accNew.TxCnt - accPrev.TxCnt)
			accTransition.StakingBlockHeight = int32(accNew.StakingBlockHeight - accPrev.StakingBlockHeight)
			stateRelative[know] = &accTransition
		}
	}
	return stateRelative
}

//Get all pubKey involved in FundsTx
func GetFundsTxPubKeys(fundsTxData [][32]byte) (fundsTxPubKeys [][32]byte) {
	for _, txHash := range fundsTxData {
		var tx protocol.Transaction
		var fundsTx *protocol.FundsTx

		tx = ReadClosedTx(txHash)
		if tx == nil {
			tx = ReadOpenTx(txHash)
		}

		fundsTx = tx.(*protocol.FundsTx)
		fundsTxPubKeys = append(fundsTxPubKeys, fundsTx.From)
		fundsTxPubKeys = append(fundsTxPubKeys, fundsTx.To)
	}

	return fundsTxPubKeys
}

//From Kürsat
func ApplyRelativeState(statePrev map[[32]byte]*protocol.Account, stateRel map[[32]byte]*protocol.RelativeAccount) (stateUpdated map[[32]byte]*protocol.Account) {
	for krel, _ := range stateRel {
		if _, ok := statePrev[krel]; !ok {
			accNewRel := stateRel[krel]
			//Important: The first argument used to be just krel, but that's not 64 bti!
			accNew := protocol.NewAccount(stateRel[krel].Address, [32]byte{}, uint64(accNewRel.Balance), accNewRel.IsStaking, accNewRel.CommitmentKey, accNewRel.Contract, accNewRel.ContractVariables)
			accNew.TxCnt = uint32(accNewRel.TxCnt)
			accNew.StakingBlockHeight = uint32(accNewRel.StakingBlockHeight)
			statePrev[krel] = &accNew
		} else {
			accPrev := statePrev[krel]
			accRel := stateRel[krel]

			//Adjust the account information
			accPrev.Balance = accPrev.Balance + uint64(accRel.Balance)
			accPrev.TxCnt = accPrev.TxCnt + uint32(accRel.TxCnt)
			accPrev.StakingBlockHeight = accPrev.StakingBlockHeight + uint32(accRel.StakingBlockHeight)
			//Staking Tx can only be positive. So only take the info from the relative state if currently not staking (otherwhise we might accidentally change the state back)
			//Also take over commitment key.
			if accPrev.IsStaking == false {
				accPrev.IsStaking = accRel.IsStaking
				accPrev.CommitmentKey = accRel.CommitmentKey
			}
		}
	}
	return statePrev
}
