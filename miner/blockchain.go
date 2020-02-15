package miner

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	logger                          *log.Logger
	blockValidation                 = &sync.Mutex{}
	epochBlockValidation            = &sync.Mutex{}
	stateTransitionValidation       = &sync.Mutex{}
	committeeCheckValidation        = &sync.Mutex{}
	transactionAssignmentValidation = &sync.Mutex{}
	parameterSlice                  []Parameters
	ActiveParameters                *Parameters
	uptodate                        bool
	slashingDict                    = make(map[[32]byte]SlashingProof)
	ValidatorAccAddress             [64]byte
	hasher                          [32]byte
	multisigPubKey                  *ecdsa.PublicKey
	commPrivKey, rootCommPrivKey    *rsa.PrivateKey
	// This map keeps track of the validator assignment to the shards
	ValidatorShardMap *protocol.ValShardMapping
	NumberOfShards    int
	// This slice stores the hashes of the last blocks from the other shards, needed to create the next epoch block.
	LastShardHashes [][32]byte

	FirstStartCommittee bool

	//Kursat Extras
	prevBlockIsEpochBlock bool
	FirstStartAfterEpoch  bool
	blockStartTime        int64
	syncStartTime         int64
	blockEndTime          int64
	totalSyncTime         int64
	NumberOfShardsDelayed int

	//Committee Slashing Extras
	ShardsToBePunished		[][32]byte
	CommitteesToBePunished	[][32]byte
)

//p2p First start entry point

func InitCommittee(committeeWallet *ecdsa.PublicKey, committeeKey *rsa.PrivateKey) {

	FirstStartAfterEpoch = true
	storage.IsCommittee = true

	//This information has to be stored as it will be important later on, for sortition purposes and for
	ValidatorAccAddress = crypto.GetAddressFromPubKey(committeeWallet)
	storage.ValidatorAccAddress = ValidatorAccAddress
	storage.CommitteePrivKey = committeeKey

	//Set up logger.
	logger = storage.InitLogger()

	logger.Printf("\n\n\n" +
		"BBBBBBBBBBBBBBBBB               AAA               ZZZZZZZZZZZZZZZZZZZ     OOOOOOOOO\n" +
		"B::::::::::::::::B             A:::A              Z:::::::::::::::::Z   OO:::::::::OO\n" +
		"B::::::BBBBBB:::::B           A:::::A             Z:::::::::::::::::Z OO:::::::::::::OO\n" +
		"BB:::::B     B:::::B         A:::::::A            Z:::ZZZZZZZZ:::::Z O:::::::OOO:::::::O\n" +
		"  B::::B     B:::::B        A:::::::::A           ZZZZZ     Z:::::Z  O::::::O   O::::::O\n" +
		"  B::::B     B:::::B       A:::::A:::::A                  Z:::::Z    O:::::O     O:::::O\n" +
		"  B::::BBBBBB:::::B       A:::::A A:::::A                Z:::::Z     O:::::O     O:::::O\n" +
		"  B:::::::::::::BB       A:::::A   A:::::A              Z:::::Z      O:::::O     O:::::O\n" +
		"  B::::BBBBBB:::::B     A:::::A     A:::::A            Z:::::Z       O:::::O     O:::::O\n" +
		"  B::::B     B:::::B   A:::::AAAAAAAAA:::::A          Z:::::Z        O:::::O     O:::::O\n" +
		"  B::::B     B:::::B  A:::::::::::::::::::::A        Z:::::Z         O:::::O     O:::::O\n" +
		"  B::::B     B:::::B A:::::AAAAAAAAAAAAA:::::A    ZZZ:::::Z     ZZZZZO::::::O   O::::::O\n" +
		"BB:::::BBBBBB::::::BA:::::A             A:::::A   Z::::::ZZZZZZZZ:::ZO:::::::OOO:::::::O\n" +
		"B:::::::::::::::::BA:::::A               A:::::A  Z:::::::::::::::::Z OO:::::::::::::OO\n" +
		"B::::::::::::::::BA:::::A                 A:::::A Z:::::::::::::::::Z   OO:::::::::OO\n" +
		"BBBBBBBBBBBBBBBBBAAAAAAA                   AAAAAAAZZZZZZZZZZZZZZZZZZZ     OOOOOOOOO\n\n\n")

	logger.Printf("\n\n\n-------------------- START Committee Member ---------------------")
	logger.Printf("This Miners IP-Address: %v\n\n", p2p.Ipport)

	currentTargetTime = new(timerange)
	target = append(target, 13)

	parameterSlice = append(parameterSlice, NewDefaultParameters())
	ActiveParameters = &parameterSlice[0]
	storage.EpochLength = ActiveParameters.Epoch_length

	//Listen for incoming blocks from the network
	go incomingData()
	//Listen for incoming epoch blocks from the network
	go incomingEpochData()
	//Listen to state transitions for validation purposes
	go incomingStateData()
	//Listen to committee check data for validation purposes
	go incomingCommitteeCheck()

	//wait for the first epoch block
	for {
		time.Sleep(time.Second)
		if lastEpochBlock != nil {
			if lastEpochBlock.Height >= 2 {
				logger.Printf("accepting the state of epoch block height: %d", lastEpochBlock.Height)
				storage.State = lastEpochBlock.State
				NumberOfShards = lastEpochBlock.NofShards
				storage.CommitteeLeader = lastEpochBlock.CommitteeLeader
				ValidatorShardMap = lastEpochBlock.ValMapping
				//initialize the assignment height
				storage.AssignmentHeight = int(lastEpochBlock.Height) - 1 - EPOCH_LENGTH
				//now initiate the committee check for the previous epoch in order to circumvent a NPE
				committeeProof, err := crypto.SignMessageWithRSAKey(storage.CommitteePrivKey, fmt.Sprint(storage.AssignmentHeight))
				if err != nil {
					logger.Printf("Got a problem with creating the committeeProof.")
					return
				}
				cc := protocol.NewCommitteeCheck(storage.AssignmentHeight, protocol.SerializeHashContent(ValidatorAccAddress), [256]byte{}, [][32]byte{}, [][32]byte{})
				copy(cc.CommitteeProof[0:crypto.COMM_PROOF_LENGTH], committeeProof[:])
				storage.OwnCommitteeCheck = cc
				break
			}
		}
	}


	//check in order to determine if all checks can be done already. If the epoch height is less than 2, a null pointer exception would happen
	if lastEpochBlock.Height == 2 {
		FirstStartCommittee = true
	} else {
		FirstStartCommittee = false
	}
	CommitteeMining(int(lastEpochBlock.Height))
}

func CommitteeMining(height int) {
	logger.Printf("---------------------------------------- Committee Mining for Epoch Height: %d ----------------------------------------", height)

	//In the beginning of each round, the slashing of the last round is performed. The reason for this is the division of power.
	//The leader of the new round should be the one who performs the checks for the last height

	//If we have more than one committee member in the blockchain, perform the slashing synchronization steps
	//If its just one, perform the check for it
	if DetNumberOfCommittees() > 1 {
		committeeMembers := getOtherCommitteeMemberAddresses()
		committeesStateBoolMap := make(map[[32]byte]bool)
		for k, _ := range committeesStateBoolMap {
			committeesStateBoolMap[k] = false
		}

		logger.Printf("We have %d Committee Nodes in the network. The commitee Leader is %x", DetNumberOfCommittees(), storage.CommitteeLeader[0:8])

		if storage.CommitteeLeader == protocol.SerializeHashContent(ValidatorAccAddress) {
			logger.Printf("I am the committee leader. Start the committee collection mechanism")

			//start the mechanism
			for {
				//Retrieve all state transitions from the local state with the height of my last block
				//height - 2 because we check for the previous epoch
				committeeCheckStashForHeight := protocol.ReturnCommitteeCheckForHeight(storage.ReceivedCommitteeCheckStash, uint32(height-2))
				if len(committeeCheckStashForHeight) != 0 {
					//Iterate through committee checks, keep track of processed checks
					for _, cc := range committeeCheckStashForHeight {
						if committeesStateBoolMap[cc.Sender] == false {
							//first check the commitment Proof. If it's invalid, continue the search
							err := validateCommitteeCheck(cc)
							if err != nil {
								logger.Printf("Cannot validate committee check")
								continue
							}
							committeesStateBoolMap[cc.Sender] = true
						}
					}
				}
				//If all committee checks have been received, stop synchronisation.
				if len(committeeCheckStashForHeight) == DetNumberOfCommittees() -1 {
					logger.Printf("Received all committee checks. Initiate the byzantine mechanism.")
					runByzantineMechanism(committeeCheckStashForHeight)
					break
				} else {
					logger.Printf("Length of committee stash: %d", len(committeeCheckStashForHeight))
				}
				//Iterate over shard IDs to check which ones are still missing, and request them from the network
				for _, address := range committeeMembers {
					if committeesStateBoolMap[address] == false {

						//Maybe the committee check was received in the meantime. Then dont request it again.
						foundCc := searchCommitteeCheck(address, height)
						if foundCc != nil {
							logger.Printf("skip planned request for address %x", address[0:8])
							continue
						}

						var committeeCheck *protocol.CommitteeCheck

						logger.Printf("requesting committeeCheck for lastblock height: %d from address: %x\n", height - 2, address)

						//-2 Because we check for the previous epoch
						p2p.CommitteeCheckReq(address, height-2)
						//Blocking wait
						select {
						case encodedCommitteeCheck := <-p2p.CommitteeCheckReqChan:
							committeeCheck = committeeCheck.DecodeCommitteeCheck(encodedCommitteeCheck)

							//double check to make sure we got the right committee check
							if committeeCheck.Sender != address || committeeCheck.Height != height -2 {
								continue
							}

							logger.Printf("Received the committee check")
							//first check the commitment Proof. If it's invalid, continue the search
							err := validateCommitteeCheck(committeeCheck)
							if err != nil {
								logger.Printf(err.Error())
								continue
							}
							storage.ReceivedCommitteeCheckStash.Set(committeeCheck.HashCommitteCheck(), committeeCheck)
							committeesStateBoolMap[committeeCheck.Sender] = true


							//Limit waiting time to 5 seconds seconds before aborting.
						case <-time.After(2 * time.Second):
							logger.Printf("have been waiting for 2 seconds for committee check height: %d\n", height-2)
							//It the requested state transition has not been received, then continue with requesting the other missing ones
							continue
						}
					}
				}
			}
		}
	} else {
		if !FirstStartCommittee {
			//give no received committee checks because I'm the only committee member
			runByzantineMechanism(nil)
		}
	}


	//generate sequence of all shard IDs starting from 1
	shardIDs := makeRange(1, NumberOfShards)
	logger.Printf("Number of shards: %d\n", NumberOfShards)
	//find out if I am the committee leader. If yes, construct the transaction assignment
	if storage.CommitteeLeader == protocol.SerializeHashContent(ValidatorAccAddress) {
		//generate sequence of all shard IDs starting from 1
		//generating the assignment data

		//Reset the map
		storage.AssignedTxMempool = make(map[[32]byte]protocol.Transaction)
		openTransactions := storage.ReadAllOpenTxs()

		logger.Printf("length of open transactions: %d", len(openTransactions))

		accTxsMap := make(map[int][]*protocol.AccTx)
		stakeTxsMap := make(map[int][]*protocol.StakeTx)
		committeeTxsMap := make(map[int][]*protocol.CommitteeTx)
		fundsTxsMap := make(map[int][]*protocol.FundsTx)
		dataTxsMap := make(map[int][]*protocol.DataTx)

		logger.Printf("before assigning transactions")

		//the transactions are distributed to the shards based on the public address of the sender
		for _, openTransaction := range openTransactions {
			//set the transaction as assigned
			storage.AssignedTxMempool[openTransaction.Hash()] = openTransaction
			switch openTransaction.(type) {
			case *protocol.AccTx:
				accTxsMap[assignTransactionToShard(openTransaction)] = append(accTxsMap[assignTransactionToShard(openTransaction)], openTransaction.(*protocol.AccTx))
			case *protocol.StakeTx:
				stakeTxsMap[assignTransactionToShard(openTransaction)] = append(stakeTxsMap[assignTransactionToShard(openTransaction)], openTransaction.(*protocol.StakeTx))
			case *protocol.CommitteeTx:
				committeeTxsMap[assignTransactionToShard(openTransaction)] = append(committeeTxsMap[assignTransactionToShard(openTransaction)], openTransaction.(*protocol.CommitteeTx))
			case *protocol.FundsTx:
				fundsTxsMap[assignTransactionToShard(openTransaction)] = append(fundsTxsMap[assignTransactionToShard(openTransaction)], openTransaction.(*protocol.FundsTx))
			case *protocol.DataTx:
				dataTxsMap[assignTransactionToShard(openTransaction)] = append(dataTxsMap[assignTransactionToShard(openTransaction)], openTransaction.(*protocol.DataTx))
			}
		}

		for _, shardId := range shardIDs {
			committeeProof, err := crypto.SignMessageWithRSAKey(storage.CommitteePrivKey, fmt.Sprint(height))
			if err != nil {
				logger.Printf("Error with signing the Committee Proof Message")
				return
			}

			ta := protocol.NewTransactionAssignment(height, shardId, committeeProof, accTxsMap[shardId], stakeTxsMap[shardId], committeeTxsMap[shardId], fundsTxsMap[shardId], dataTxsMap[shardId])

			storage.AssignedTxMap[shardId] = ta
			logger.Printf("broadcasting assignment data for ShardId: %d", shardId)
			logger.Printf("Length of AccTx: %d, StakeTx: %d, CommitteeTx: %d, FundsTx: %d, DataTx: %d", len(accTxsMap[shardId]), len(stakeTxsMap[shardId]), len(committeeTxsMap[shardId]), len(fundsTxsMap[shardId]), len(dataTxsMap[shardId]))
			broadcastAssignmentData(ta)
		}

		logger.Printf("After assigning transactions")
		//If I am not the committee leader, wait for the assignment
	} else {
		//reset the assigned tx map
		storage.AssignedTxMempool = make(map[[32]byte]protocol.Transaction)
		shardIDs := makeRange(1, NumberOfShards)
		// receive all transaction assignments for the height
		shardIDBoolMap := make(map[int]bool)
		for k, _ := range shardIDBoolMap {
			shardIDBoolMap[k] = false
		}
		for {
			//Retrieve all state transitions from the local state with the height of my last block
			transactionAssignmentsForHeight := protocol.ReturnTransactionAssignmentForHeight(storage.ReceivedTransactionAssignmentStash, uint32(height))
			if len(transactionAssignmentsForHeight) != 0 {
				//Iterate through committee checks, keep track of processed checks
				for _, ta := range transactionAssignmentsForHeight {
					if shardIDBoolMap[ta.ShardID] == false {
						//first check the commitment Proof. If it's invalid, continue the search
						err := validateTransactionAssignment(ta)
						if err != nil {
							logger.Printf("Cannot validate transaction assignment")
							continue
						}
						storage.AssignedTxMap[ta.ShardID]= ta

						//overwrite the previous mempool. Take the new transactions
						//this is thread safe because it's all done sequentially
						//it's important so we can have all TXs for prevalidation
						for _, transaction := range ta.AccTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range ta.StakeTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range ta.CommitteeTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range ta.FundsTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range ta.DataTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						shardIDBoolMap[ta.ShardID] = true
					}
				}
			}
			//If all state transitions have been received, stop synchronisation
			if len(transactionAssignmentsForHeight) == NumberOfShards {
				logger.Printf("Received all transaction assignments. Continue")
				break
			} else {
				logger.Printf("Length of transaction assignment stash: %d", len(transactionAssignmentsForHeight))
			}
			//Iterate over shard IDs to check which ones are still missing, and request them from the network
			for _, id := range shardIDs {
				if shardIDBoolMap[id] == false {

					//Maybe the committee check was received in the meantime. Then dont request it again.
					foundTa := searchTransactionAssignment(id, height)
					if foundTa != nil {
						logger.Printf("skip planned request for id %d", id)
						continue
					}

					var transactionAssignment *protocol.TransactionAssignment


					p2p.TransactionAssignmentReq(height, id)
					//Blocking wait
					select {
					case encodedTransactionAssignment := <-p2p.TransactionAssignmentReqChan:
						transactionAssignment = transactionAssignment.DecodeTransactionAssignment(encodedTransactionAssignment)

						//double check to make sure that the received TA is actually what we are looking for
						if transactionAssignment.ShardID != id || transactionAssignment.Height != height{
							continue
						}

						//first check the commitment Proof. If it's invalid, continue the search
						err := validateTransactionAssignment(transactionAssignment)
						if err != nil {
							logger.Printf(err.Error())
							continue
						}

						storage.AssignedTxMap[transactionAssignment.ShardID]= transactionAssignment

						//overwrite the previous mempool. Take the new transactions
						//this is thread safe because it's all done sequentially
						//it's important so we can have all TXs for prevalidation
						for _, transaction := range transactionAssignment.AccTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range transactionAssignment.StakeTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range transactionAssignment.CommitteeTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range transactionAssignment.FundsTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}
						for _, transaction := range transactionAssignment.DataTxs {
							storage.AssignedTxMempool[transaction.Hash()] = transaction
						}

						storage.ReceivedTransactionAssignmentStash.Set(transactionAssignment.HashTransactionAssignment(), transactionAssignment)

						shardIDBoolMap[transactionAssignment.ShardID] = true

						//Limit waiting time to 5 seconds seconds before aborting.
					case <-time.After(2 * time.Second):
						logger.Printf("have been waiting for 2 seconds for transaction assignment height: %d\n", height)
						//It the requested state transition has not been received, then continue with requesting the other missing ones
						continue
					}
				}
			}
		}
	}
	storage.AssignmentHeight = height

	//let the goroutine collect the state transitions in the background and contionue with the block collection
	waitGroup := sync.WaitGroup{}
	go fetchStateTransitionsForHeight(height+1, &waitGroup)

	//key: shard ID; value: Relative state of the corresponding shard
	relativeStatesToCheck := make(map[int]*protocol.RelativeState)

	blockIDBoolMap := make(map[int]bool)
	for k, _ := range blockIDBoolMap {
		blockIDBoolMap[k] = false
	}

	//no block validation in the first round to make sure that the genesis block isn't checked
	if !FirstStartCommittee || DetNumberOfCommittees() > 1 {
		logger.Printf("before block validation")
		for {
			//the committee member is now bootstrapped. In an infinite for-loop, perform its task
			blockStashForHeight := protocol.ReturnBlockStashForHeight(storage.ReceivedShardBlockStash, uint32(height+1))
			if len(blockStashForHeight) != 0 {
				logger.Printf("height being inspected: %d", height+1)
				logger.Printf("length of block stash for height: %d", len(blockStashForHeight))
				//Iterate through state transitions and apply them to local state, keep track of processed shards
				//Also perform some verification steps, i.e. proof of stake check
				for _, b := range blockStashForHeight {
					if blockIDBoolMap[b.ShardId] == false {

						//keep looking if the sender cannot be verified
						if !ValidateBlockSender(b){
							continue
						}

						err := CommitteeValidateBlock(b)
						if err != nil {
							ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
						}

						//fetch data from the block
						accTxs, fundsTxs, _, stakeTxs, committeeTxs, aggTxs, aggregatedFundsTxSlice, dataTxs, aggregatedDataTxSlice, aggDataTxs, err := preValidate(b, false)

						//append the aggTxs to the normal fundsTxs to delete
						fundsTxs = append(fundsTxs, aggregatedFundsTxSlice...)
						dataTxs = append(dataTxs, aggregatedDataTxSlice...)



						relativeState := ReconstructRelativeState(b, accTxs, stakeTxs, committeeTxs, fundsTxs, dataTxs)
						relativeStatesToCheck[b.ShardId] = relativeState

						UpdateSummary(dataTxs)

						logger.Printf("In block from shardID: %d, height: %d, deleting accTxs: %d, stakeTxs: %d, committeeTxs: %d, fundsTxs: %d, aggTxs: %d, dataTxs: %d, aggDataTxs: %d", b.ShardId, b.Height, len(accTxs), len(stakeTxs), len(committeeTxs), len(fundsTxs), len(aggTxs), len(dataTxs), len(aggDataTxs))


						alreadyClosedTxHashes, err := storage.WriteAllClosedTxAndReturnAlreadyClosedTxHashes(accTxs, stakeTxs, committeeTxs, fundsTxs, aggTxs, dataTxs, aggDataTxs)
						if err != nil {
							logger.Printf(err.Error())
							return
						}
						if len(alreadyClosedTxHashes) > 0{
							for _,hash := range alreadyClosedTxHashes{
								//check if the transaction was in the assignment
								if _, ok := storage.AssignedTxMempool[hash]; ok {
									//the transaction is in the assignment, punish the committee leader
									CommitteesToBePunished = append(CommitteesToBePunished, storage.CommitteeLeader)
								} else {
									//the transaction is not in the assignment, punish the shard
									ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
								}
							}
						}

						storage.ReadAllOpenTxs()


						notIncludedTxHashes := storage.DeleteAllOpenTxAndReturnAllNotIncludedTxHashes(accTxs, stakeTxs, committeeTxs, fundsTxs, aggTxs, dataTxs, aggDataTxs)
						//If this evaluates to true, then the shard created a transaction out of thin air.
						if len(notIncludedTxHashes) > 0 {
							logger.Printf("found a shard to be punished")
							ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
						}

						blockIDBoolMap[b.ShardId] = true

						logger.Printf("Processed block of shard: %d\n", b.ShardId)

					}
				}
				//If all blocks have been received, stop synchronisation
				if len(blockStashForHeight) == NumberOfShards {
					logger.Printf("received all blocks for height. Break")
					break
				} else {
					logger.Printf("height: %d", height+1)
					logger.Printf("number of shards: %d", NumberOfShards)
				}
			}
			//for the blocks that haven't been processed yet, introduce request structure
			//can still accelerate this structure
			for _, shardIdReq := range shardIDs {
				if !blockIDBoolMap[shardIdReq] {
					var b *protocol.Block

					foundBlock := searchBlock(shardIdReq, height + 1)
					if foundBlock != nil {
						logger.Printf("skip planned block request for shardID %d", shardIdReq)
						continue
					}

					logger.Printf("Requesting Block for Height: %d and ShardID %d", int(height)+1, shardIdReq)
					p2p.ShardBlockReq(int(height)+1, shardIdReq)
					//blocking wait
					select {
					//received the response, perform the verification and write in map
					case encodedBlock := <-p2p.ShardBlockReqChan:
						b = b.Decode(encodedBlock)

						if b == nil {
							logger.Printf("block is nil")
							return
						}

						if b.ShardId != shardIdReq {
							logger.Printf("Shard ID of received block %d vs shard ID of request %d. Continue", b.ShardId, shardIdReq)
							continue
						}

						//keep looking if the sender cannot be verified
						if !ValidateBlockSender(b){
							continue
						}

						logger.Printf("Validation of block height: %d, ShardID: %d", b.Height, b.ShardId)

						err := CommitteeValidateBlock(b)
						if err != nil {
							ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
						}

						//fetch data from the block
						accTxs, fundsTxs, _, stakeTxs, committeeTxs, aggTxs, aggregatedFundsTxSlice, dataTxs, aggregatedDataTxSlice, aggDataTxs, err := preValidate(b, false)

						//append the aggTxs to the normal fundsTxs to delete
						fundsTxs = append(fundsTxs, aggregatedFundsTxSlice...)
						dataTxs = append(dataTxs, aggregatedDataTxSlice...)

						UpdateSummary(dataTxs)


						relativeState := ReconstructRelativeState(b, accTxs, stakeTxs, committeeTxs, fundsTxs, dataTxs)
						relativeStatesToCheck[b.ShardId] = relativeState


						alreadyClosedTxHashes, err := storage.WriteAllClosedTxAndReturnAlreadyClosedTxHashes(accTxs, stakeTxs, committeeTxs, fundsTxs, aggTxs, dataTxs, aggDataTxs)
						if err != nil {
							logger.Printf(err.Error())
							return
						}
						if len(alreadyClosedTxHashes) > 0{
							for _,hash := range alreadyClosedTxHashes{
								//check if the transaction was in the assignment
								if _, ok := storage.AssignedTxMempool[hash]; ok {
									//the transaction is in the assignment, punish the committee leader
									CommitteesToBePunished = append(CommitteesToBePunished, storage.CommitteeLeader)
								} else {
									//the transaction is not in the assignment, punish the shard
									ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
								}
							}
						}

						notIncludedTxHashes := storage.DeleteAllOpenTxAndReturnAllNotIncludedTxHashes(accTxs, stakeTxs, committeeTxs, fundsTxs, aggTxs, dataTxs, aggDataTxs)

					//If this evaluates to true, then the shard created a transaction out of thin air.
						if len(notIncludedTxHashes) > 0 {
							ShardsToBePunished = append(ShardsToBePunished, b.Beneficiary)
						}

						blockIDBoolMap[shardIdReq] = true

						//store the block in the received block stash as well
						blockHash := b.HashBlock()
						if storage.ReceivedShardBlockStash.BlockIncluded(blockHash) == false {
							logger.Printf("Writing block to stash Shard ID: %v  - Height: %d - Hash: %x\n", b.ShardId, b.Height, blockHash[0:8])
							storage.ReceivedShardBlockStash.Set(blockHash, b)
						}

						logger.Printf("Processed block of shard: %d\n", b.ShardId)

					case <-time.After(2 * time.Second):
						logger.Printf("waited 2 seconds for lastblock height: %d, shardID: %d", int(height)+1, shardIdReq)
						logger.Printf("Broadcast Epoch Block to bootstrap new nodes")
						broadcastEpochBlock(lastEpochBlock)
					}
				}
			}
		}
		logger.Printf("end of block validation for height: %d", storage.AssignmentHeight)
	}

	//wait until the state transitions are all in storage.
	logger.Printf("Waiting for all state transitions to arrive")
	waitGroup.Wait()
	logger.Printf("All state transitions already received")

	//go through all state transitions and compare them with the actual transactions inside the block
	stateStashForHeight := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, uint32(height+1))
	for _, st := range stateStashForHeight {
		ownRelativeState := relativeStatesToCheck[st.ShardID]
		if !sameRelativeState(st.RelativeStateChange, ownRelativeState.RelativeState) {
			logger.Printf("FOUND A CHEATER: Shard %d", st.ShardID)
			ShardsToBePunished = append(ShardsToBePunished, ownRelativeState.Beneficiary)
		} else {
			logger.Printf("For Shard ID: %d the relative states match", st.ShardID)
		}
	}


	logger.Printf("Wait for next epoch block")
	//wait for next epoch block
	epochBlockReceived := false
	for !epochBlockReceived {
		logger.Printf("waiting for epoch block height %d", uint32(storage.AssignmentHeight)+1+EPOCH_LENGTH)
		newEpochBlock := <-p2p.EpochBlockReceivedChan
		logger.Printf("received the desired epoch block")
		if newEpochBlock.Height == uint32(storage.AssignmentHeight)+1+EPOCH_LENGTH {

			//check if the sender of the epoch block is legit
			if !ValidateEpochBlockSender(&newEpochBlock) {
				continue
			}

			//broadcastEpochBlock(storage.ReadLastClosedEpochBlock())
			epochBlockReceived = true

			//since it's safely not the first step of mining anymore, it's safe to perform proof of stake at this step
			err := validateEpochBlock(&newEpochBlock, relativeStatesToCheck)
			if err != nil {
				//no further actions to be taken because the slashing already happens inside the validation function
				logger.Printf(err.Error())
			} else {
				logger.Printf("The Epoch Block and its state are valid")
			}

			//before being able to validate the proof of stake, the state needs to updated
			storage.State = newEpochBlock.State
			storage.CommitteeLeader = newEpochBlock.CommitteeLeader
			ValidatorShardMap = newEpochBlock.ValMapping
			NumberOfShards = newEpochBlock.NofShards
		}
	}
	committeeProof, err := crypto.SignMessageWithRSAKey(storage.CommitteePrivKey, fmt.Sprint(storage.AssignmentHeight))
	if err != nil {
		logger.Printf("Got a problem with creating the committeeProof.")
		return
	}
	//broadcast committee check to the network
	committeeCheck := protocol.NewCommitteeCheck(storage.AssignmentHeight, protocol.SerializeHashContent(ValidatorAccAddress), committeeProof, CommitteesToBePunished, ShardsToBePunished)
	copy(committeeCheck.CommitteeProof[0:crypto.COMM_PROOF_LENGTH], committeeProof[:])
	storage.OwnCommitteeCheck = committeeCheck
	//only broadcast if I am not the leader
	broadcastCommitteeCheck(committeeCheck)

	logger.Printf("Length of committees to be slashed: %d, length of shards to be slashed: %d", len(CommitteesToBePunished), len(ShardsToBePunished))

	//Reset the slices where the slashed addresses are saved
	CommitteesToBePunished = [][32]byte{}
	ShardsToBePunished = [][32]byte{}

	FirstStartCommittee = false
	//Empty the map
	storage.AssignedTxMap = make(map[int]*protocol.TransactionAssignment)
	logger.Printf("Received epoch block. Start next round")
	CommitteeMining(int(lastEpochBlock.Height))
}

// Doesnt do much, just bootstraps the system with the things that are needed for the first start, i.e. a genesis block and a first epoch block.
func InitFirstStart(validatorWallet, multisigWallet, rootWallet *ecdsa.PublicKey, validatorCommitment, rootCommitment *rsa.PrivateKey) error {
	var err error
	if err != nil {
		return err
	}
	firstCommitteePubKey, err := crypto.ExtractECDSAPublicKeyFromFile("WalletCommitteeA.txt")
	if err != nil {
		logger.Printf("%v\n", err)
		return err
	}

	firstCommitteeComPrivKey, err := crypto.ExtractRSAKeyFromFile("CommitteeA.txt")
	if err != nil {
		logger.Printf("%v\n", err)
		return err
	}

	rootAddress := crypto.GetAddressFromPubKey(rootWallet)
	firstCommitteeAddress := crypto.GetAddressFromPubKey(firstCommitteePubKey)

	var rootCommitmentKey [crypto.COMM_KEY_LENGTH]byte
	copy(rootCommitmentKey[:], rootCommitment.N.Bytes())

	var firstCommitteeKey [crypto.COMM_KEY_LENGTH]byte
	copy(firstCommitteeKey[:], firstCommitteeComPrivKey.N.Bytes())

	genesis := protocol.NewGenesis(rootAddress, rootCommitmentKey, firstCommitteeAddress, firstCommitteeKey)
	storage.WriteGenesis(&genesis)

	/*Write First Epoch block chained to the genesis block*/
	initialEpochBlock := protocol.NewEpochBlock([][32]byte{genesis.Hash()}, 0)
	initialEpochBlock.Hash = initialEpochBlock.HashEpochBlock()
	FirstEpochBlock = initialEpochBlock
	initialEpochBlock.State = storage.State

	storage.WriteFirstEpochBlock(initialEpochBlock)

	storage.WriteClosedEpochBlock(initialEpochBlock)

	storage.DeleteAllLastClosedEpochBlock()
	storage.WriteLastClosedEpochBlock(initialEpochBlock)

	firstValMapping := protocol.NewMapping()
	initialEpochBlock.ValMapping = firstValMapping

	return Init(validatorWallet, multisigWallet, rootWallet, validatorCommitment, rootCommitment)
}

//Miner entry point
func Init(validatorWallet, multisigWallet, rootWallet *ecdsa.PublicKey, validatorCommitment, rootCommitment *rsa.PrivateKey) error {
	var err error

	ValidatorAccAddress = crypto.GetAddressFromPubKey(validatorWallet)
	multisigPubKey = multisigWallet
	commPrivKey = validatorCommitment
	rootCommPrivKey = rootCommitment
	storage.IsCommittee = false

	//Set up logger.
	logger = storage.InitLogger()
	hasher = protocol.SerializeHashContent(ValidatorAccAddress)
	logger.Printf("Acc hash is (%x)", hasher[0:8])
	logger.Printf("\n\n\n" +
		"BBBBBBBBBBBBBBBBB               AAA               ZZZZZZZZZZZZZZZZZZZ     OOOOOOOOO\n" +
		"B::::::::::::::::B             A:::A              Z:::::::::::::::::Z   OO:::::::::OO\n" +
		"B::::::BBBBBB:::::B           A:::::A             Z:::::::::::::::::Z OO:::::::::::::OO\n" +
		"BB:::::B     B:::::B         A:::::::A            Z:::ZZZZZZZZ:::::Z O:::::::OOO:::::::O\n" +
		"  B::::B     B:::::B        A:::::::::A           ZZZZZ     Z:::::Z  O::::::O   O::::::O\n" +
		"  B::::B     B:::::B       A:::::A:::::A                  Z:::::Z    O:::::O     O:::::O\n" +
		"  B::::BBBBBB:::::B       A:::::A A:::::A                Z:::::Z     O:::::O     O:::::O\n" +
		"  B:::::::::::::BB       A:::::A   A:::::A              Z:::::Z      O:::::O     O:::::O\n" +
		"  B::::BBBBBB:::::B     A:::::A     A:::::A            Z:::::Z       O:::::O     O:::::O\n" +
		"  B::::B     B:::::B   A:::::AAAAAAAAA:::::A          Z:::::Z        O:::::O     O:::::O\n" +
		"  B::::B     B:::::B  A:::::::::::::::::::::A        Z:::::Z         O:::::O     O:::::O\n" +
		"  B::::B     B:::::B A:::::AAAAAAAAAAAAA:::::A    ZZZ:::::Z     ZZZZZO::::::O   O::::::O\n" +
		"BB:::::BBBBBB::::::BA:::::A             A:::::A   Z::::::ZZZZZZZZ:::ZO:::::::OOO:::::::O\n" +
		"B:::::::::::::::::BA:::::A               A:::::A  Z:::::::::::::::::Z OO:::::::::::::OO\n" +
		"B::::::::::::::::BA:::::A                 A:::::A Z:::::::::::::::::Z   OO:::::::::OO\n" +
		"BBBBBBBBBBBBBBBBBAAAAAAA                   AAAAAAAZZZZZZZZZZZZZZZZZZZ     OOOOOOOOO\n\n\n")

	logger.Printf("\n\n\n-------------------- START MINER ---------------------")
	logger.Printf("This Miners IP-Address: %v\n\n", p2p.Ipport)
	time.Sleep(2 * time.Second)
	parameterSlice = append(parameterSlice, NewDefaultParameters())
	ActiveParameters = &parameterSlice[0]
	storage.EpochLength = ActiveParameters.Epoch_length

	//Initialize root key.
	initRootKey(rootWallet)
	if err != nil {
		logger.Printf("Could not create a root account.\n")
	}

	currentTargetTime = new(timerange)
	target = append(target, 13)

	logger.Printf("ActiveConfigParams: \n%v\n------------------------------------------------------------------------\n\nBAZO is Running\n\n", ActiveParameters)

	//this is used to generate the state with aggregated transactions.
	for _, tx := range storage.ReadAllBootstrapReceivedTransactions() {
		if tx != nil {
			storage.DeleteOpenTx(tx)
			storage.WriteClosedTx(tx)
		}
	}
	storage.DeleteBootstrapReceivedMempool()

	var initialBlock *protocol.Block

	//Listen for incoming epoch blocks from the network
	go incomingEpochData()
	//Listen for incoming assignments from the network
	go incomingTransactionAssignment()
	//Listen for incoming state transitions from the network
	go incomingStateData()

	//Since new validators only join after the currently running epoch ends, they do no need to download the whole shardchain history,
	//but can continue with their work after the next epoch block and directly set their state to the global state of the first received epoch block

	if p2p.IsBootstrap() {
		initialBlock, err = initState() //From here on, every validator should have the same state representation
		if err != nil {
			return err
		}
		lastBlock = initialBlock
	} else {
		for {
			//As the non-bootstrapping node, wait until I receive the last epoch block as well as the validator assignment
			// The global variables 'lastEpochBlock' and 'ValidatorShardMap' are being set when they are received by the network
			//seems the timeout is needed for nodes to be able to access
			time.Sleep(time.Second)
			if lastEpochBlock != nil {
				logger.Printf("First statement ok")
				if lastEpochBlock.Height > 0 {
					storage.State = lastEpochBlock.State
					NumberOfShards = lastEpochBlock.NofShards
					ValidatorShardMap = lastEpochBlock.ValMapping
					storage.ThisShardID = ValidatorShardMap.ValMapping[ValidatorAccAddress] //Save my ShardID
					storage.ThisShardMap[int(lastEpochBlock.Height)] = storage.ThisShardID
					FirstStartAfterEpoch = true
					storage.CommitteeLeader = lastEpochBlock.CommitteeLeader
					lastBlock = dummyLastBlock
					epochMining(lastEpochBlock.Hash, lastEpochBlock.Height) //start mining based on the received Epoch Block
					//set the ID to 0 such that there wont be any answers to requests that shouldnt be answered
					storage.ThisShardIDDelayed = 0
				}
			}
		}
	}

	logger.Printf("Active config params:%v\n", ActiveParameters)

	//Define number of shards based on the validators in the network
	NumberOfShards = DetNumberOfShards()
	logger.Printf("Number of Shards: %v", NumberOfShards)

	/*First validator assignment is done by the bootstrapping node, the others will be done based on PoS at the end of each epoch*/
	if p2p.IsBootstrap() {
		var validatorShardMapping = protocol.NewMapping()
		validatorShardMapping.ValMapping = AssignValidatorsToShards()
		validatorShardMapping.EpochHeight = int(lastEpochBlock.Height)
		ValidatorShardMap = validatorShardMapping
		storage.CommitteeLeader = ChooseCommitteeLeader()
		logger.Printf("Validator Shard Mapping:\n")
		logger.Printf(validatorShardMapping.String())
	}

	storage.ThisShardID = ValidatorShardMap.ValMapping[ValidatorAccAddress]
	storage.ThisShardMap[int(lastEpochBlock.Height)] = storage.ThisShardID
	epochMining(lastBlock.Hash, lastBlock.Height)

	return nil
}

/**
Main function of Bazo which is running all the time with the goal of mining blocks.
*/
func epochMining(hashPrevBlock [32]byte, heightPrevBlock uint32) {

	var epochBlock *protocol.EpochBlock

	for {
		//Indicates that a validator newly joined Bazo after the current epoch, thus his 'lastBlock' variable is nil
		//and he continues directly with the mining of the first shard block
		if FirstStartAfterEpoch {
			logger.Printf("First start after Epoch. New miner successfully introduced to Bazo network")
			mining(hashPrevBlock, heightPrevBlock)
		}

		if lastBlock.Height == uint32(lastEpochBlock.Height)+uint32(ActiveParameters.Epoch_length) {
			if storage.ThisShardID == 1 {

				shardIDs := makeRange(1, NumberOfShards)

				shardIDStateBoolMap := make(map[int]bool)
				for k, _ := range shardIDStateBoolMap {
					shardIDStateBoolMap[k] = false
				}

				for {
					//If there is only one shard, then skip synchronisation mechanism
					if NumberOfShards == 1 {
						break
					}

					//Retrieve all state transitions from the local state with the height of my last block
					stateStashForHeight := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, lastBlock.Height)

					if len(stateStashForHeight) != 0 {
						//Iterate through state transitions and apply them to local state, keep track of processed shards
						for _, st := range stateStashForHeight {
							if shardIDStateBoolMap[st.ShardID] == false && st.ShardID != storage.ThisShardID {

								//first check the commitment Proof. If it's invalid, continue the search
								err := validateStateTransition(st)
								if err != nil {
									logger.Printf(err.Error())
									continue
								}

								//Apply all relative account changes to my local state
								storage.State = storage.ApplyRelativeState(storage.State, st.RelativeStateChange)
								shardIDStateBoolMap[st.ShardID] = true
								logger.Printf("Processed state transition of shard: %d\n", st.ShardID)
							}
						}
						//If all state transitions have been received, stop synchronisation
						if len(stateStashForHeight) == NumberOfShards-1 {
							logger.Printf("Received all transitions. Continue.")
							break
						} else {
							logger.Printf("Length of state stash: %d", len(stateStashForHeight))
						}
					}
					//Iterate over shard IDs to check which ones are still missing, and request them from the network
					for _, id := range shardIDs {
						if id != storage.ThisShardID && shardIDStateBoolMap[id] == false {

							//Maybe the transition was received in the meantime. Then dont request it again.
							foundSt := searchStateTransition(id, int(lastBlock.Height))
							if foundSt != nil {
								logger.Printf("skip planned request for shardID %d", id)
								continue
							}

							var stateTransition *protocol.StateTransition

							logger.Printf("requesting state transition for lastblock height: %d\n", lastBlock.Height)

							p2p.StateTransitionReqShard(id, int(lastBlock.Height))
							//Blocking wait
							select {
							case encodedStateTransition := <-p2p.StateTransitionShardReqChan:
								stateTransition = stateTransition.DecodeTransition(encodedStateTransition)

								//first check the commitment Proof. If it's invalid, continue the search
								err := validateStateTransition(stateTransition)
								if err != nil {
									logger.Printf(err.Error())
									continue
								}

								//Apply state transition to my local state
								storage.State = storage.ApplyRelativeState(storage.State, stateTransition.RelativeStateChange)

								logger.Printf("Writing state back to stash Shard ID: %v  VS my shard ID: %v - Height: %d\n", stateTransition.ShardID, storage.ThisShardID, stateTransition.Height)
								storage.ReceivedStateStash.Set(stateTransition.HashTransition(), stateTransition)

								//Delete transactions from mempool, which were validated by the other shards

								shardIDStateBoolMap[stateTransition.ShardID] = true

								logger.Printf("Processed state transition of shard: %d\n", stateTransition.ShardID)

								//Limit waiting time to 5 seconds seconds before aborting.
							case <-time.After(2 * time.Second):
								logger.Printf("have been waiting for 5 seconds for lastblock height: %d\n", lastBlock.Height)
								//It the requested state transition has not been received, then continue with requesting the other missing ones
								continue
							}
						}
					}
				}

				//After the state transition mechanism is finished, perform the epoch block creation

				epochBlock = protocol.NewEpochBlock([][32]byte{lastBlock.Hash}, lastBlock.Height+1)
				logger.Printf("epochblock beingprocessed height: %d\n", epochBlock.Height)

				logger.Printf("Before finalizeEpochBlock() ---- Height: %d\n", epochBlock.Height)
				//Finalize creation of the epoch block. In case another epoch block was mined in the meantime, abort PoS here

				//add the beneficiary to the epoch block
				validatorAcc, err := storage.GetAccount(protocol.SerializeHashContent(ValidatorAccAddress))
				if err != nil {
					logger.Printf("problem with getting the validator acc")
				}

				validatorAccHash := validatorAcc.Hash()

				copy(epochBlock.Beneficiary[:], validatorAccHash[:])

				err = finalizeEpochBlock(epochBlock)

				logger.Printf("After finalizeEpochBlock() ---- Height: %d\n", epochBlock.Height)

				if err != nil {
					logger.Printf("%v\n", err)
				} else {
					logger.Printf("EPOCH BLOCK mined (%x)\n", epochBlock.Hash[0:8])
				}

				//Successfully mined epoch block
				if err == nil {
					logger.Printf("Broadcast epoch block (%x)\n", epochBlock.Hash[0:8])
					//Broadcast epoch block to other nodes such that they can update their validator-shard assignment
					broadcastEpochBlock(epochBlock)
					storage.WriteClosedEpochBlock(epochBlock)
					storage.DeleteAllLastClosedEpochBlock()
					storage.WriteLastClosedEpochBlock(epochBlock)
					lastEpochBlock = epochBlock

					logger.Printf("Created Validator Shard Mapping :\n")
					logger.Printf(ValidatorShardMap.String())
					logger.Printf("Inserting EPOCH BLOCK: %v\n", epochBlock.String())
					logger.Printf("Created Validator Shard Mapping :\n")
					logger.Printf(ValidatorShardMap.String() + "\n")

					for _, prevHash := range epochBlock.PrevShardHashes {
						//FileConnections.WriteString(fmt.Sprintf("'%x' -> 'EPOCH BLOCK: %x'\n", prevHash[0:15], epochBlock.Hash[0:15]))
						logger.Printf(`"Hash : %x \n Height : %d" -> "EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+"\n", prevHash[0:8], epochBlock.Height-1, epochBlock.Hash[0:8], epochBlock.Height, epochBlock.MerklePatriciaRoot[0:8])
						logger.Printf(`"EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+`[color = red, shape = box]`+"\n", epochBlock.Hash[0:8], epochBlock.Height, epochBlock.MerklePatriciaRoot[0:8])
					}
				}

				// I'm not shard number one so I just wait until I receive the next epoch block
			} else {
				//wait until epoch block is received
				epochBlockReceived := false
				for !epochBlockReceived {
					newEpochBlock := <-p2p.EpochBlockReceivedChan
					//the new epoch block from the channel is the epoch block that i need at the moment
					if newEpochBlock.Height == lastBlock.Height+1 {
						//check if the sender of the epoch block is legit
						if !ValidateEpochBlockSender(&newEpochBlock) {
							continue
						}
						epochBlockReceived = true
						// take over state
						storage.State = newEpochBlock.State
						ValidatorShardMap = newEpochBlock.ValMapping
						NumberOfShards = newEpochBlock.NofShards
						storage.CommitteeLeader = newEpochBlock.CommitteeLeader
						storage.ThisShardID = ValidatorShardMap.ValMapping[ValidatorAccAddress]
						storage.ThisShardMap[int(newEpochBlock.Height)] = storage.ThisShardID
						lastEpochBlock = &newEpochBlock
						logger.Printf("Received  last epoch block with height %d. Continue mining", lastEpochBlock.Height)
					}
				}
			}
			prevBlockIsEpochBlock = true
			firstEpochOver = true
			received := false
			//now delete old assignment and wait to receive the assignment from the committee
			storage.AssignedTxMempool = make(map[[32]byte]protocol.Transaction)
			//Blocking wait
			logger.Printf("Wait for transaction assignment")
			for {
				select {
				case encodedTransactionAssignment := <-p2p.TransactionAssignmentReqChan:
					var transactionAssignment *protocol.TransactionAssignment
					transactionAssignment = transactionAssignment.DecodeTransactionAssignment(encodedTransactionAssignment)

					//got the transaction assignment for the wrong height. Request again.
					if transactionAssignment.Height != int(lastEpochBlock.Height) {
						time.Sleep(2 * time.Second)
						p2p.TransactionAssignmentReq(int(lastEpochBlock.Height), storage.ThisShardID)
						logger.Printf("Assignment height: %d vs epoch height %d", transactionAssignment.Height, epochBlock.Height)
						continue
					}

					//Check the signature inside the assignment.
					err := validateTransactionAssignment(transactionAssignment)
					if err != nil {
						logger.Printf(err.Error())
						continue
					} else {
						logger.Printf("The received transaction assignment is valid")
					}

					//overwrite the previous mempool. Take the new transactions
					//this is thread safe because it's all done sequentially
					for _, transaction := range transactionAssignment.AccTxs {
						storage.AssignedTxMempool[transaction.Hash()] = transaction
					}
					for _, transaction := range transactionAssignment.StakeTxs {
						storage.AssignedTxMempool[transaction.Hash()] = transaction
					}
					for _, transaction := range transactionAssignment.CommitteeTxs {
						storage.AssignedTxMempool[transaction.Hash()] = transaction
					}
					for _, transaction := range transactionAssignment.FundsTxs {
						storage.AssignedTxMempool[transaction.Hash()] = transaction
					}
					for _, transaction := range transactionAssignment.DataTxs {
						storage.AssignedTxMempool[transaction.Hash()] = transaction
					}
					logger.Printf("Success. Received assignment for height: %d", transactionAssignment.Height)
					received = true
				case <-time.After(2 * time.Second):
					logger.Printf("Requesting transaction assignment for shard ID: %d with height: %d", storage.ThisShardID, lastEpochBlock.Height)
					p2p.TransactionAssignmentReq(int(lastEpochBlock.Height), storage.ThisShardID)
					//this is used to bootstrap the committee.
					broadcastEpochBlock(lastEpochBlock)
				}
				if received {
					break
				}
			}

			logger.Printf("received both my transaction assignment and the epoch block. can continue now")

			//Continue mining with the hash of the last epoch block
			mining(lastEpochBlock.Hash, lastEpochBlock.Height)
		} else if lastEpochBlock.Height == lastBlock.Height+1 {
			prevBlockIsEpochBlock = true
			mining(lastEpochBlock.Hash, lastEpochBlock.Height) //lastblock was received before we started creation of next epoch block
		} else {
			mining(lastBlock.Hash, lastBlock.Height)
		}
	}
}

//Mining is a constant process, trying to come up with a successful PoW.
func mining(hashPrevBlock [32]byte, heightPrevBlock uint32) {

	logger.Printf("\n\n __________________________________________________ New Mining Round __________________________________________________")
	logger.Printf("Create Next Block")
	//This is the same mutex that is claimed at the beginning of a block validation. The reason we do this is
	//that before start mining a new block we empty the mempool which contains tx data that is likely to be
	//validated with block validation, so we wait in order to not work on tx data that is already validated
	//when we finish the block.
	blockValidation.Lock()
	currentBlock := newBlock(hashPrevBlock, [crypto.COMM_PROOF_LENGTH]byte{}, heightPrevBlock+1)

	//Set shard identifier in block (not necessary? It's already written inside the block)
	currentBlock.ShardId = storage.ThisShardID
	logger.Printf("This shard ID: %d", storage.ThisShardID)

	logger.Printf("Prepare Next Block")
	prepareBlock(currentBlock)
	blockValidation.Unlock()
	logger.Printf("Prepare Next Block --> Done")
	blockBeingProcessed = currentBlock
	logger.Printf("Finalize Next Block")
	err := finalizeBlock(currentBlock)

	logger.Printf("Finalize Next Block -> Done. Block height: %d", blockBeingProcessed.Height)
	if err != nil {
		logger.Printf("%v\n", err)
	} else {
		logger.Printf("Block mined (%x)\n", currentBlock.Hash[0:8])
	}

	if err == nil {
		err := validate(currentBlock, false)
		if err == nil {
			//only the shards which do not create the epoch block need to send out a state transition
			if storage.ThisShardID != 1 {
				commitmentProof, err := crypto.SignMessageWithRSAKey(commPrivKey, fmt.Sprint(currentBlock.Height))
				if err != nil {
					logger.Printf("Got a problem with creating the commimentProof.")
					return
				}
				stateTransition := protocol.NewStateTransition(storage.RelativeState, int(currentBlock.Height), storage.ThisShardID, commitmentProof)
				copy(stateTransition.CommitmentProof[0:crypto.COMM_PROOF_LENGTH], commitmentProof[:])
				storage.WriteToOwnStateTransitionkStash(stateTransition)
				broadcastStateTransition(stateTransition)
			}
			broadcastBlock(currentBlock)
			logger.Printf("Validated block (mined): %vState:\n%v", currentBlock, getState())
		} else {
			logger.Printf("Mined block (%x) could not be validated: %v\n", currentBlock.Hash[0:8], err)
		}
	}

	//Prints miner connections
	p2p.EmptyingiplistChan()
	p2p.PrintMinerConns()

	FirstStartAfterEpoch = false
	NumberOfShardsDelayed = NumberOfShards
	storage.ThisShardIDDelayed = storage.ThisShardID

}

//At least one root key needs to be set which is allowed to create new accounts.
func initRootKey(rootKey *ecdsa.PublicKey) error {
	address := crypto.GetAddressFromPubKey(rootKey)
	addressHash := protocol.SerializeHashContent(address)

	var commPubKey [crypto.COMM_KEY_LENGTH]byte
	copy(commPubKey[:], rootCommPrivKey.N.Bytes())

	rootAcc := protocol.NewAccount(address, [32]byte{}, ActiveParameters.Staking_minimum, true, false, commPubKey, [crypto.COMM_KEY_LENGTH]byte{}, nil, nil)
	storage.State[addressHash] = &rootAcc
	storage.RootKeys[addressHash] = &rootAcc

	return nil
}

func DetNumberOfCommittees() (numberOfCommittees int) {
	return GetCommitteesCount()
}

func getOtherCommitteeMemberAddresses() (committeeMembers [][32]byte) {

	for address, account := range storage.State {
		//only add to list if its not the own address
		if account.IsCommittee && account.Address != ValidatorAccAddress {
			committeeMembers = append(committeeMembers, address)
		}
	}
	if len(committeeMembers) != DetNumberOfCommittees() - 1 {
		logger.Printf("Error in this function")
	}

	return committeeMembers
}

func ChooseCommitteeLeader() (committeeLeader [32]byte) {
	//randomize the process
	rand.Seed(time.Now().Unix())
	//rand.Intn gives me a random number [0,n). For example, if there is only 1 committee, then, it's [0,1) = [0,0] = 0, since the numbers are discrete

	//make sure that newly joining committees can be the leader (simplifies synchronization)
	if newCommitteeNode != [64]byte{} {
		return protocol.SerializeHashContent(newCommitteeNode)
	}

	randomIndex := rand.Intn(DetNumberOfCommittees())
	committeeSlice := make([][32]byte, 0)
	for _, acc := range storage.State {
		if acc.IsCommittee {
			committeeSlice = append(committeeSlice, protocol.SerializeHashContent(acc.Address))
		}
	}
	return committeeSlice[randomIndex]
}

/**
Number of Shards is determined based on the total number of validators in the network. Currently, the system supports only
one validator per shard, thus Number of Shards = Number of Validators.
*/
func DetNumberOfShards() (numberOfShards int) {
	return int(math.Ceil(float64(GetValidatorsCount()) / float64(ActiveParameters.validators_per_shard)))
}

/**
This function assigns the validators to the single shards in a random fashion. In case multiple validators per shard are supported,
they would be assigned to the shards uniformly.
*/
func AssignValidatorsToShards() map[[64]byte]int {

	logger.Printf("Assign validators to shards start")
	/*This map denotes which validator is assigned to which shard index*/
	validatorShardAssignment := make(map[[64]byte]int)

	/*Fill 'validatorAssignedMap' with the validators of the current state.
	The bool value indicates whether the validator has been assigned to a shard
	*/
	validatorSlices := make([][64]byte, 0)
	validatorAssignedMap := make(map[[64]byte]bool)
	for _, acc := range storage.State {
		if acc.IsStaking {
			validatorAssignedMap[acc.Address] = false
			validatorSlices = append(validatorSlices, acc.Address)
		}
	}

	/*Iterate over range of shards. At each index, select a random validators
	from the map above and set is bool 'assigned' to TRUE*/
	rand.Seed(time.Now().Unix())

	for j := 1; j <= int(ActiveParameters.validators_per_shard); j++ {
		for i := 1; i <= NumberOfShards; i++ {

			//finished the process of assigning the validators to shards
			if len(validatorSlices) == 0 {

				return validatorShardAssignment
			}

			randomIndex := rand.Intn(len(validatorSlices))
			randomValidator := validatorSlices[randomIndex]

			//Assign validator to shard ID
			validatorShardAssignment[randomValidator] = i
			//Remove assigned validator from active list
			validatorSlices = removeValidator(validatorSlices, randomIndex)
		}
	}
	return validatorShardAssignment
}

func searchStateTransition(shardID int, height int) *protocol.StateTransition {
	stateStash := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, uint32(height))
	for _, st := range stateStash {
		if st.ShardID == shardID {
			return st
		}
	}
	return nil
}

func searchBlock(shardID int, height int) *protocol.Block {
	blockStash := protocol.ReturnBlockStashForHeight(storage.ReceivedShardBlockStash, uint32(height))
	for _, b := range blockStash {
		if b.ShardId == shardID {
			return b
		}
	}
	return nil
}

func searchCommitteeCheck(address [32]byte, height int) *protocol.CommitteeCheck {
	committeeStash := protocol.ReturnCommitteeCheckForHeight(storage.ReceivedCommitteeCheckStash, uint32(height))
	for _,cc := range committeeStash {
		if cc.Sender == address {
			return cc
		}
	}
	return nil
}

func searchTransactionAssignment(shardId int, height int) *protocol.TransactionAssignment {
	transactionAssignmentStash := protocol.ReturnTransactionAssignmentForHeight(storage.ReceivedTransactionAssignmentStash, uint32(height))
	for _,ta := range transactionAssignmentStash {
		if ta.ShardID == shardId {
			return ta
		}
	}
	return nil
}

func fetchStateTransitionsForHeight(height int, group *sync.WaitGroup) {
	//block the wait group until the function stops execution
	group.Add(1)
	defer group.Done()
	// if there is only one, shard, then no state transitions will be in the system to be fetched
	logger.Printf("Start fetch state transitions for height: %d. Number of Shards: %d", height, NumberOfShards)
	if NumberOfShards == 1 {
		return
	} else {
		shardIDs := makeRange(1, NumberOfShards)
		shardIDStateBoolMap := make(map[int]bool)
		for k, _ := range shardIDStateBoolMap {
			shardIDStateBoolMap[k] = false
		}

		//start the mechanism
		for {
			//Retrieve all state transitions from the local state with the height of my last block
			stateStashForHeight := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, uint32(height))
			if len(stateStashForHeight) != 0 {
				//Iterate through state transitions and apply them to local state, keep track of processed shards
				for _, st := range stateStashForHeight {
					if shardIDStateBoolMap[st.ShardID] == false {
						//first check the commitment Proof. If it's invalid, continue the search
						err := validateStateTransition(st)
						if err != nil {
							logger.Printf("Cannot validate state transition")
							continue
						}
						//Apply all relative account changes to my local state
						shardIDStateBoolMap[st.ShardID] = true
					}
				}
				//If all state transitions have been received, stop synchronisation
				if len(stateStashForHeight) == NumberOfShards-1 {
					logger.Printf("Received all transitions. Continue.")
					return
				} else {
					logger.Printf("Length of state stash: %d", len(stateStashForHeight))
				}
				//Iterate over shard IDs to check which ones are still missing, and request them from the network
				for _, id := range shardIDs {
					if shardIDStateBoolMap[id] == false {

						//Maybe the transition was received in the meantime. Then dont request it again.
						foundSt := searchStateTransition(id, height)
						if foundSt != nil {
							logger.Printf("skip planned request for shardID %d", id)
							continue
						}

						var stateTransition *protocol.StateTransition

						logger.Printf("requesting state transition for lastblock height: %d\n", height)

						p2p.StateTransitionReqShard(id, height)
						//Blocking wait
						select {
						case encodedStateTransition := <-p2p.StateTransitionShardReqChan:
							stateTransition = stateTransition.DecodeTransition(encodedStateTransition)

							//first check the commitment Proof. If it's invalid, continue the search
							err := validateStateTransition(stateTransition)
							if err != nil {
								logger.Printf(err.Error())
								continue
							}

							storage.ReceivedStateStash.Set(stateTransition.HashTransition(), stateTransition)

							shardIDStateBoolMap[stateTransition.ShardID] = true

							//Limit waiting time to 5 seconds seconds before aborting.
						case <-time.After(2 * time.Second):
							logger.Printf("have been waiting for 2 seconds for lastblock height: %d\n", height)
							//It the requested state transition has not been received, then continue with requesting the other missing ones
							continue
						}
					}
				}
			}
		}
	}
}

func applyDataTxFees(state map[[32]byte]protocol.Account, beneficiary [32]byte, dataTxs []*protocol.DataTx) (map[[32]byte]protocol.Account, error) {
	var err error
	//the beneficiary stays the same for one round
	minerAcc := state[beneficiary]
	for _, tx := range dataTxs {
		if minerAcc.Balance+tx.Fee > MAX_MONEY {
			err = errors.New("Fee amount would lead to balance overflow at the miner account.")
		}
		senderAcc := state[tx.From]

		senderAcc.Balance -= tx.Fee
		minerAcc.Balance += tx.Fee
		state[tx.From] = senderAcc
		state[beneficiary] = minerAcc
	}
	return state, err
}

func applyAccTxFeesAndCreateAccTx(state map[[32]byte]protocol.Account, beneficiary [32]byte, accTxs []*protocol.AccTx) (map[[32]byte]protocol.Account, error) {
	var err error
	//the beneficiary stays the same for one round
	minerAcc := state[beneficiary]
	for _, tx := range accTxs {
		if minerAcc.Balance+tx.Fee > MAX_MONEY {
			err = errors.New("Fee amount would lead to balance overflow at the miner account.")
		}
		//For accTxs, new funds have to be produced
		minerAcc.Balance += tx.Fee
		state[beneficiary] = minerAcc
		//create the account and add it to the account
		newAcc := protocol.NewAccount(tx.PubKey, tx.Issuer, 100000, false, false, [crypto.COMM_KEY_LENGTH]byte{}, [crypto.COMM_KEY_LENGTH]byte{}, tx.Contract, tx.ContractVariables)
		newAccHash := newAcc.Hash()
		state[newAccHash] = newAcc
	}
	return state, err
}

func applyCommitteeTxFeesAndCreateAcc(state map[[32]byte]protocol.Account, beneficiary [32]byte, committeeTxs []*protocol.CommitteeTx) (map[[32]byte]protocol.Account, error) {
	var err error
	//the beneficiary stays the same for one round
	minerAcc := state[beneficiary]
	for _, tx := range committeeTxs {
		if minerAcc.Balance+tx.Fee > MAX_MONEY {
			err = errors.New("Fee amount would lead to balance overflow at the miner account.")
		}
		//For accTxs, new funds have to be produced
		minerAcc.Balance += tx.Fee
		state[beneficiary] = minerAcc
		//create the account and add it to the account
		newAcc := protocol.NewAccount(tx.Account, tx.Issuer, 0, false, true, [crypto.COMM_KEY_LENGTH]byte{}, tx.CommitteeKey, nil, nil)
		newAccHash := newAcc.Hash()
		state[newAccHash] = newAcc
	}
	return state, err
}

func applyStakeTxFees(state map[[32]byte]protocol.Account, beneficiary [32]byte, stakeTxs []*protocol.StakeTx) (map[[32]byte]protocol.Account, error) {
	var err error
	//the beneficiary stays the same for one round
	minerAcc := state[beneficiary]
	for _, tx := range stakeTxs {
		if minerAcc.Balance+tx.Fee > MAX_MONEY {
			err = errors.New("Fee amount would lead to balance overflow at the miner account.")
		}
		senderAcc := state[tx.Account]
		senderAcc.Balance -= tx.Fee
		minerAcc.Balance += tx.Fee
		state[tx.Account] = senderAcc
		state[beneficiary] = minerAcc
	}
	return state, err
}

func applyFundsTxFeesFundsMovement(state map[[32]byte]protocol.Account, beneficiary [32]byte, fundsTxs []*protocol.FundsTx) (map[[32]byte]protocol.Account, error) {
	var err error
	//the beneficiary stays the same for one round
	minerAcc := state[beneficiary]
	for _, tx := range fundsTxs {
		if minerAcc.Balance+tx.Fee > MAX_MONEY {
			err = errors.New("Fee amount would lead to balance overflow at the miner account.")
		}
		//Partition the process in case the sender/receiver are the same as the beneficiary
		//first handle amount
		senderAcc := state[tx.From]
		receiverAcc := state[tx.To]
		senderAcc.Balance -= tx.Amount
		receiverAcc.Balance += tx.Amount
		state[tx.To] = receiverAcc
		state[tx.From] = senderAcc
		//now handle the fee
		minerAcc := state[beneficiary]
		senderAcc = state[tx.From]
		senderAcc.Balance -= tx.Fee
		minerAcc.Balance += tx.Fee
		state[tx.From] = senderAcc
		state[beneficiary] = minerAcc
	}
	return state, err
}

func sameRelativeState(calculatedMap map[[32]byte]*protocol.RelativeAccount, receivedMap map[[32]byte]*protocol.RelativeAccount) bool {
	//at the moment, we only care about funds. This, however could be extended in the future
	for account, _ := range calculatedMap {
		if calculatedMap[account].Balance != receivedMap[account].Balance {
			logger.Printf("Calculated MAP: Account %x has balance %d ----- Received MAP has balance %d", account[0:8], calculatedMap[account].Balance, receivedMap[account].Balance)
			return false
		}
	}
	return true
}

func UpdateSummary(dataTxs []*protocol.DataTx) {
	//only iterate through data Txs once, so write summary AND check fee just once.
	if len(dataTxs) > 0 {
		err := storage.UpdateDataSummary(dataTxs)
		if err != nil {
			logger.Printf("Error when updating the data summary")
			return
		} else {
			logger.Printf("Data Summary Updated")
			newDataSummarySlice := storage.ReadAllDataSummary()
			if len(newDataSummarySlice) == 0 {
				logger.Printf("got a problem!!")
				return
			}
			//logger.Printf("Start Print data summary")
			//for _, dataSummary := range newDataSummarySlice {
			//logger.Printf(dataSummary.String())
			//}
		}
	}
}

func ReconstructRelativeState(b *protocol.Block, accTxs []*protocol.AccTx, stakeTxs []*protocol.StakeTx, committeeTxs []*protocol.CommitteeTx, fundsTxs []*protocol.FundsTx, dataTxs []*protocol.DataTx) *protocol.RelativeState {
	//here create the state copy and calculate the relative state
	//for this purpose, only the flow of funds has to be analyzed
	var StateCopy = CopyState(storage.State)
	var StateOld = CopyState(storage.State)

	//Shard 1 has more transactions to check
	//order matters
	//if b.ShardId == 1 {
	if true {
		StateCopy, _ = applyAccTxFeesAndCreateAccTx(StateCopy, b.Beneficiary, accTxs)
		StateCopy, _ = applyCommitteeTxFeesAndCreateAcc(StateCopy, b.Beneficiary, committeeTxs)
		StateCopy, _ = applyStakeTxFees(StateCopy, b.Beneficiary, stakeTxs)
		StateCopy, _ = applyFundsTxFeesFundsMovement(StateCopy, b.Beneficiary, fundsTxs)
	}

	//the fees are applied on the state copy
	StateCopy, _ = applyDataTxFees(StateCopy, b.Beneficiary, dataTxs)

	relativeStateProvisory := storage.GetRelativeStateForCommittee(StateOld, StateCopy)

	return protocol.NewRelativeState(relativeStateProvisory, b.ShardId, b.Beneficiary)
}

func ValidateEpochBlockSender(b *protocol.EpochBlock) bool {
	//Check state contains beneficiary.
	acc, err := storage.GetAccount(b.Beneficiary)
	if err != nil {
		return false
	}

	if !acc.IsStaking {
		return false
	}

	for address, shardId := range ValidatorShardMap.ValMapping {
		//because shard 1 is responsible for epoch block creation
		if shardId == 1 {
			if protocol.SerializeHashContent(address) == b.Beneficiary {
				commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
				if err != nil {
					return false
				}
				err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(b.Height), b.CommitmentProof)
				if err != nil {
					return false
				}
				return true
			}
		}
	}
	return false
}

func ValidateBlockSender(b *protocol.Block) bool {
	//Check state contains beneficiary.
	acc, err := storage.GetAccount(b.Beneficiary)
	if err != nil {
		return false
	}

	if !acc.IsStaking {
		return false
	}

	for address, shardId := range ValidatorShardMap.ValMapping {
		if shardId == b.ShardId {
			if protocol.SerializeHashContent(address) == b.Beneficiary {
				commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
				if err != nil {
					return false
				}
				err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(b.Height), b.CommitmentProof)
				if err != nil {
					return false
				}
				return true
			}
		}
	}
	return false
}

func runByzantineMechanism(receivedCC []*protocol.CommitteeCheck) {
	//determine the number of committee members. If only one, just work with own stash
	numberOfCommittees := DetNumberOfCommittees()
	//now determine how many votes are required for slashing. The percentage needed for slashing can be set in the config file.
	numberOfVotesForSlashing := DetNumberOfVotersForSlashing(numberOfCommittees)
	//fineMapShards keeps the addresses of all shards along with the number of committee members that voted to slash it
	fineMapShards := make(map[[32]byte]int)
	//fineMapCommittees keeps the addresses of all committee members along with the number of committee members that voted to slash it
	fineMapCommittees := make(map[[32]byte]int)
	//add own committee check to the slice
	receivedCC = append(receivedCC, storage.OwnCommitteeCheck)
	logger.Printf("Amount of Committee members: %d", numberOfCommittees)
	for _, account := range storage.State {
		if account.IsStaking {
			//iterate through the other committee checks
			for _, cc := range receivedCC {
				if containsAddress(cc.SlashedAddressesShards, protocol.SerializeHashContent(account.Address)) {
					//vote to slash the account
					fineMapShards[protocol.SerializeHashContent(account.Address)] += 1
				}
			}
		}
		if account.IsCommittee {
			//iterate through the other committee checks
			for _, cc := range receivedCC {
				if containsAddress(cc.SlashedAddressesCommittee, protocol.SerializeHashContent(account.Address)) {
					//vote to slash the account
					fineMapCommittees[protocol.SerializeHashContent(account.Address)] += 1
				}
			}
		}
	}
	//now the maps contain the desired information
	for address, votes := range fineMapShards {
		if votes >= int(numberOfVotesForSlashing) {
			logger.Printf("The committee decided to slash %x with %d votes in favor of slashing", address[0:8], votes)
		} else {
			logger.Printf("The committee decided not to slash %x because there weren't enough votes: %d.", address[0:8], votes)
		}
	}
	for address, votes := range fineMapCommittees {
		if votes >= int(numberOfVotesForSlashing) {
			logger.Printf("The committee decided to slash %x with %d votes in favor of slashing", address[0:8], votes)
		} else {
			logger.Printf("The committee decided not to slash %x because there weren't enough votes: %d", address[0:8], votes)
		}
	}
	logger.Printf("End of Byzantine slashing mechanism")
}

func DetNumberOfVotersForSlashing(numberOfCommittees int) int {
	return int(math.Ceil(float64(numberOfCommittees) * PERCENTAGE_NEEDED_FOR_SLASHING))
}

func CommitteeValidateBlock(b *protocol.Block) (err error) {
	logger.Printf("Validation of block height: %d, ShardID: %d", b.Height, b.ShardId)

	//Check state contains beneficiary.
	acc, err := storage.GetAccount(b.Beneficiary)
	if err != nil {
		return errors.New("Don't have the beneficiary")
	}

	//Check if node is part of the validator set.
	if !acc.IsStaking {
		return errors.New("Account isn't staking")
	}

	//First, initialize an RSA Public Key instance with the modulus of the proposer of the block (acc)
	//Second, check if the commitment proof of the proposed block can be verified with the public key
	//Invalid if the commitment proof can not be verified with the public key of the proposer
	commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
	if err != nil {
		return errors.New("commitment key cannot be retrieved")
	}

	err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(b.Height), b.CommitmentProof)
	logger.Printf("CommitmentPubKey: %x, --------------- Block Height: %d", commitmentPubKey, b.Height)
	if err != nil {
		return errors.New("The submitted commitment proof can not be verified.")
	}

	//Invalid if PoS calculation is not correct.
	prevProofs := GetLatestProofs(ActiveParameters.num_included_prev_proofs, b)
	if validateProofOfStake(getDifficulty(), prevProofs, b.Height, acc.Balance, b.CommitmentProof, b.Timestamp) {
		logger.Printf("proof of stake is valid")
	} else {
		return errors.New("proof of stake is invalid")
	}
	return nil
}

//Helper functions

func removeValidator(inputSlice [][64]byte, index int) [][64]byte {
	inputSlice[index] = inputSlice[len(inputSlice)-1]
	inputSlice = inputSlice[:len(inputSlice)-1]
	return inputSlice
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func containsAddress(slice [][32]byte, add [32]byte) bool {
	for _, address := range slice {
		if address == add {
			return true
		}
	}
	return false
}
