package miner

import (
	"crypto/ecdsa"
	"crypto/rsa"
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
	logger                       *log.Logger
	blockValidation              = &sync.Mutex{}
	epochBlockValidation	     = &sync.Mutex{}
	parameterSlice               []Parameters
	ActiveParameters 			*Parameters
	uptodate                     bool
	slashingDict                 = make(map[[32]byte]SlashingProof)
	validatorAccAddress          [64]byte
	hasher                       [32]byte
	multisigPubKey               *ecdsa.PublicKey
	commPrivKey, rootCommPrivKey *rsa.PrivateKey
	// This map keeps track of the validator assignment to the shards
	ValidatorShardMap *protocol.ValShardMapping
	NumberOfShards    int
	// This slice stores the hashes of the last blocks from the other shards, needed to create the next epoch block.
	LastShardHashes [][32]byte

	FirstStartCommittee  bool

	//Kursat Extras
	prevBlockIsEpochBlock bool
	FirstStartAfterEpoch  bool
	blockStartTime        int64
	syncStartTime         int64
	blockEndTime          int64
	totalSyncTime         int64
	NumberOfShardsDelayed int
)

//p2p First start entry point


func InitCommittee() {

	FirstStartAfterEpoch = true
	storage.IsCommittee = true

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

	//wait for the first epoch block
	for {
		time.Sleep(time.Second)
		if (lastEpochBlock != nil) {
			if (lastEpochBlock.Height >= 2) {
				logger.Printf("accepting the state of epoch block height: %d", lastEpochBlock.Height)
				storage.State = lastEpochBlock.State
				NumberOfShards = lastEpochBlock.NofShards
				break
			}
		}
	}
	FirstStartCommittee = true
	CommitteeMining(int(lastEpochBlock.Height))
}



func CommitteeMining(height int) {
	logger.Printf("---------------------------------------- Committee Mining for Epoch Height: %d ----------------------------------------", height)
	blockIDBoolMap := make(map[int]bool)
	for k, _ := range blockIDBoolMap {
		blockIDBoolMap[k] = false
	}

	//generate sequence of all shard IDs starting from 1
	shardIDs := makeRange(1,NumberOfShards)
	logger.Printf("Number of shards: %d\n",NumberOfShards)

	//generating the assignment data
	logger.Printf("before assigning transactions")
	for _, shardId := range shardIDs {
		var ta *protocol.TransactionAssignment
		var accTxs []*protocol.AccTx
		var stakeTxs []*protocol.StakeTx
		var fundsTxs []*protocol.FundsTx

		openTransactions := storage.ReadAllOpenTxs()

		//empty the assignment and all the slices
		ta = nil
		accTxs = nil
		stakeTxs = nil
		fundsTxs = nil


		//since shard number 1 writes the epoch block, it is required to process all acctx and stake tx
		//the other transactions are distributed to the shards based on the public address of the sender
		for _, openTransaction := range openTransactions {
			switch openTransaction.(type) {
			case *protocol.AccTx:
				if shardId == 1 {
					accTxs = append(accTxs, openTransaction.(*protocol.AccTx))
				}
			case *protocol.StakeTx:
				if shardId == 1 {
					stakeTxs = append(stakeTxs, openTransaction.(*protocol.StakeTx))
				}
			case *protocol.FundsTx:
				if shardId == assignTransactionToShard(openTransaction) {
					fundsTxs = append(fundsTxs, openTransaction.(*protocol.FundsTx))
				}
			}
		}
		ta = protocol.NewTransactionAssignment(height, shardId, accTxs, stakeTxs, fundsTxs)

		logger.Printf("length of open transactions: %d", len(storage.ReadAllOpenTxs()))
		storage.AssignedTxMap[shardId] = ta
		logger.Printf("broadcasting assignment data for ShardId: %d", shardId)
		logger.Printf("Length of AccTx: %d, StakeTx: %d, FundsTx: %d", len(accTxs), len(stakeTxs), len(fundsTxs))
		broadcastAssignmentData(ta)
	}
	storage.AssignmentHeight = height
	logger.Printf("After assigning transactions")

	//no block validation in the first round to make sure that the genesis block isn't checked
	if !FirstStartCommittee {
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

						blockIDBoolMap[b.ShardId] = true

						logger.Printf("Validation of block height: %d, ShardID: %d", b.Height, b.ShardId)

						//Check state contains beneficiary.
						acc, err := storage.GetAccount(b.Beneficiary)
						if err != nil {
							logger.Printf("Don't have the beneficiary")
							return
						}

						//Check if node is part of the validator set.
						if !acc.IsStaking {
							logger.Printf("Account isn't staking")
							return
						}

						//First, initialize an RSA Public Key instance with the modulus of the proposer of the block (acc)
						//Second, check if the commitment proof of the proposed block can be verified with the public key
						//Invalid if the commitment proof can not be verified with the public key of the proposer
						commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
						if err != nil {
							logger.Printf("commitment key cannot be retrieved")
							return
						}

						err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(b.Height), b.CommitmentProof)
						logger.Printf("CommitmentPubKey: %x, --------------- Block Height: %d", commitmentPubKey, b.Height)
						if err != nil {
							logger.Printf("The submitted commitment proof can not be verified.")
							return
						}

						//Invalid if PoS calculation is not correct.
						prevProofs := GetLatestProofs(ActiveParameters.num_included_prev_proofs, b)
						validateProofOfStake(getDifficulty(), prevProofs, b.Height, acc.Balance, b.CommitmentProof, b.Timestamp)

						logger.Printf("proof of stake is valid")

						accTxs, fundsTxs, _, stakeTxs, _, aggregatedFundsTxSlice, err := preValidate(b, false)

						fundsTxs = append(fundsTxs, aggregatedFundsTxSlice...)

						storage.WriteAllClosedTx(accTxs, stakeTxs, fundsTxs)
						storage.DeleteAllOpenTx(accTxs, stakeTxs, fundsTxs)

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
					logger.Printf("Requesting Block for Height: %d and ShardID %d",int(height)+1, shardIdReq)
					p2p.ShardBlockReq(int(height)+1, shardIdReq)
					//blocking wait
					select {
					//received the response, perform the verification and write in map
					case encodedBlock := <-p2p.ShardBlockReqChan:
						b = b.Decode(encodedBlock)

						if b == nil {
							logger.Printf("block is nil")
						}

						if b.ShardId != shardIdReq {
							logger.Printf("Shard ID of received block %d vs shard ID of request %d. Continue", b.ShardId, shardIdReq)
							continue
						}
						blockIDBoolMap[shardIdReq] = true

						logger.Printf("Validation of block height: %d, ShardID: %d", b.Height, b.ShardId)

						//Check state contains beneficiary.
						acc, err := storage.GetAccount(b.Beneficiary)
						if err != nil {
							logger.Printf("Don't have the beneficiary")
							return
						}

						//Check if node is part of the validator set.
						if !acc.IsStaking {
							logger.Printf("Account isn't staking")
							return
						}

						//First, initialize an RSA Public Key instance with the modulus of the proposer of the block (acc)
						//Second, check if the commitment proof of the proposed block can be verified with the public key
						//Invalid if the commitment proof can not be verified with the public key of the proposer
						commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
						if err != nil {
							logger.Printf("commitment key cannot be retrieved")
							return
						}

						err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(b.Height), b.CommitmentProof)
						logger.Printf("CommitmentPubKey: %x, --------------- Block Height: %d", commitmentPubKey, b.Height)
						if err != nil {
							logger.Printf("The submitted commitment proof can not be verified.")
							return
						}

						//Invalid if PoS calculation is not correct.
						prevProofs := GetLatestProofs(ActiveParameters.num_included_prev_proofs, b)
						validateProofOfStake(getDifficulty(), prevProofs, b.Height, acc.Balance, b.CommitmentProof, b.Timestamp)

						logger.Printf("proof of stake is valid")

						accTxs, fundsTxs, _, stakeTxs, _, aggregatedFundsTxSlice, err := preValidate(b, false)

						fundsTxs = append(fundsTxs, aggregatedFundsTxSlice...)

						storage.WriteAllClosedTx(accTxs, stakeTxs, fundsTxs)
						storage.DeleteAllOpenTx(accTxs, stakeTxs, fundsTxs)

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
	//wait for next epoch block
	epochBlockReceived := false
	for !epochBlockReceived {
		logger.Printf("waiting for epoch block height %d", uint32(storage.AssignmentHeight)+1+EPOCH_LENGTH)
		newEpochBlock := <-p2p.EpochBlockReceivedChan
		logger.Printf("received the desired epoch block")
		if newEpochBlock.Height == uint32(storage.AssignmentHeight)+1+EPOCH_LENGTH {
			//broadcastEpochBlock(storage.ReadLastClosedEpochBlock())
			epochBlockReceived = true
			storage.State = lastEpochBlock.State
			NumberOfShards = lastEpochBlock.NofShards
		}
	}
	FirstStartCommittee = false
	logger.Printf("Received epoch block. Start next round")
	CommitteeMining(int(lastEpochBlock.Height))
}


func InitFirstStart(validatorWallet, multisigWallet, rootWallet *ecdsa.PublicKey, validatorCommitment, rootCommitment *rsa.PrivateKey) error {
	var err error
	if err != nil {
		return err
	}

	rootAddress := crypto.GetAddressFromPubKey(rootWallet)

	var rootCommitmentKey [crypto.COMM_KEY_LENGTH]byte
	copy(rootCommitmentKey[:], rootCommitment.N.Bytes())

	genesis := protocol.NewGenesis(rootAddress, rootCommitmentKey)
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

	validatorAccAddress = crypto.GetAddressFromPubKey(validatorWallet)
	multisigPubKey = multisigWallet
	commPrivKey = validatorCommitment
	rootCommPrivKey = rootCommitment
	storage.IsCommittee = false

	//Set up logger.
	logger = storage.InitLogger()
	hasher = protocol.SerializeHashContent(validatorAccAddress)
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

	/* TODO not sure if it's ok to remove this. This is removed because we have init state later down
	initialBlock, err := initState()
	if err != nil {
		logger.Printf("Could not set up initial state: %v.\n", err)
		return
	}
	*/
	logger.Printf("ActiveConfigParams: \n%v\n------------------------------------------------------------------------\n\nBAZO is Running\n\n", ActiveParameters)

	//this is used to generate the state with aggregated transactions.
	//TODO is this still necessary after the local state of the sharding is introduced?
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


	//Since new validators only join after the currently running epoch ends, they do no need to download the whole shardchain history,
	//but can continue with their work after the next epoch block and directly set their state to the global state of the first received epoch block


	if (p2p.IsBootstrap()) {
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
			if (lastEpochBlock != nil && ValidatorShardMap != nil) {
				logger.Printf("First statement ok")
				if (lastEpochBlock.Height > 0) {
					storage.State = lastEpochBlock.State
					NumberOfShards = lastEpochBlock.NofShards
					storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress] //Save my ShardID
					storage.ThisShardMap[int(lastEpochBlock.Height)] = storage.ThisShardID
					FirstStartAfterEpoch = true
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
	if (p2p.IsBootstrap()) {
		var validatorShardMapping = protocol.NewMapping()
		validatorShardMapping.ValMapping = AssignValidatorsToShards()
		validatorShardMapping.EpochHeight = int(lastEpochBlock.Height)
		ValidatorShardMap = validatorShardMapping
		logger.Printf("Validator Shard Mapping:\n")
		logger.Printf(validatorShardMapping.String())
	}

	storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress]
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


		if (lastBlock.Height == uint32(lastEpochBlock.Height)+uint32(ActiveParameters.Epoch_length)) {
			if (storage.ThisShardID == 1) {
				epochBlock = protocol.NewEpochBlock([][32]byte{lastBlock.Hash}, lastBlock.Height+1)
				logger.Printf("epochblock beingprocessed height: %d\n", epochBlock.Height)

				if (NumberOfShards != 1) {
					//Extract the hashes of the last blocks of the other shards, needed to create the epoch block
					//The hashes of the blocks are stored in the state transitions of the other shards
					LastShardHashes = protocol.ReturnShardHashesForHeight(storage.ReceivedStateStash, lastBlock.Height)
					epochBlock.PrevShardHashes = append(epochBlock.PrevShardHashes, LastShardHashes...)
				}

				logger.Printf("Before finalizeEpochBlock() ---- Height: %d\n", epochBlock.Height)
				//Finalize creation of the epoch block. In case another epoch block was mined in the meantime, abort PoS here


				//add the beneficiary to the epoch block
				validatorAcc, err := storage.GetAccount(protocol.SerializeHashContent(validatorAccAddress))
				if err != nil {
					logger.Printf("problem with getting the validator acc")
				}

				validatorAccHash := validatorAcc.Hash()

				logger.Printf("validator acc hash: %x", validatorAccHash)

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
					logger.Printf("Inserting EPOCH BLOCK: %v\n", epochBlock.String())

					for _, prevHash := range epochBlock.PrevShardHashes {
						//FileConnections.WriteString(fmt.Sprintf("'%x' -> 'EPOCH BLOCK: %x'\n", prevHash[0:15], epochBlock.Hash[0:15]))
						logger.Printf(`"Hash : %x \n Height : %d" -> "EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+"\n", prevHash[0:8], epochBlock.Height-1, epochBlock.Hash[0:8], epochBlock.Height, epochBlock.MerklePatriciaRoot[0:8])
						logger.Printf(`"EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+`[color = red, shape = box]`+"\n", epochBlock.Hash[0:8], epochBlock.Height, epochBlock.MerklePatriciaRoot[0:8])
					}
				}

				//Introduce some delay in case there was a fork of the epoch block.
				//Even though the states of both epoch blocks are the same, the validator-shard assignment is likely to be different
				//General rule: Accept the last received epoch block as the valid one.
				//Idea: We just accept the last received epoch block. There is no rollback for epoch blocks in place.
				//Kürsat hopes that the last received Epoch block will be the same for all blocks.
				//This pseudo sortition mechanism of waiting probably wont be needed anymore
				//time.Sleep(5 * time.Second)
			// I'm not shard number one so I just wait until I receive the next epoch block
			} else {
				//wait until epoch block is received
				epochBlockReceived := false
				for !epochBlockReceived {
					newEpochBlock := <- p2p.EpochBlockReceivedChan
					if newEpochBlock.Height == lastBlock.Height + 1 {
						epochBlockReceived = true
					}
				}
			}
			prevBlockIsEpochBlock = true
			firstEpochOver = true
			received := false
			//now delete old assignment and wait to receive the assignment from the committee
			storage.AssignedTxMempool = nil
			//Blocking wait
			logger.Printf("Wait for transaction assignment")
			for {
				select {
				case encodedTransactionAssignment := <-p2p.TransactionAssignmentReqChan:
					var transactionAssignment *protocol.TransactionAssignment
					transactionAssignment = transactionAssignment.DecodeTransactionAssignment(encodedTransactionAssignment)
					//overwrite the previous mempool. Take the new transactions
					for _, transaction := range transactionAssignment.AccTxs {
						storage.AssignedTxMempool = append(storage.AssignedTxMempool, transaction)
					}
					for _, transaction := range transactionAssignment.StakeTxs {
						storage.AssignedTxMempool = append(storage.AssignedTxMempool, transaction)
					}
					for _, transaction := range transactionAssignment.FundsTxs {
						storage.AssignedTxMempool = append(storage.AssignedTxMempool, transaction)
					}
					logger.Printf("Success. Received assignment for height: %d", transactionAssignment.Height)
					received = true
				case <-time.After(5 * time.Second):
					logger.Printf("Requesting transaction assignment for shard ID: %d with height: %d", storage.ThisShardID, lastEpochBlock.Height)
					p2p.TransactionAssignmentReq(int(lastEpochBlock.Height), storage.ThisShardID)
				}
				if received {
					break
				}
			}

			logger.Printf("received both my transaction assignment and the epoch block. can continue now")


			//Continue mining with the hash of the last epoch block
			mining(lastEpochBlock.Hash, lastEpochBlock.Height)
		} else if (lastEpochBlock.Height == lastBlock.Height+1) {
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

			//Generate state transition for this block. This data is needed by the other shards to update their local states.
			//use the freshly updated shardId, because the block always has to be in the new epoch already. If it was in the old epoch,
			//the epoch block would not have been generated either
			stateTransition := protocol.NewStateTransition(storage.RelativeState, int(currentBlock.Height), storage.ThisShardID, currentBlock.Hash,
				currentBlock.AccTxData, currentBlock.ContractTxData, currentBlock.FundsTxData, currentBlock.ConfigTxData, currentBlock.StakeTxData, currentBlock.AggTxData)


			logger.Printf("Transactions to delete in other miners count: %d - New Mempool Size: %d\n",len(stateTransition.ContractTxData)+len(stateTransition.FundsTxData)+len(stateTransition.ConfigTxData)+ len(stateTransition.StakeTxData) + len(stateTransition.AggTxData),storage.GetMemPoolSize())

			logger.Printf("Broadcast state transition for height %d\n", currentBlock.Height)
			//Broadcast state transition to other shards
			broadcastStateTransition(stateTransition)
			//Write state transition to own stash. Needed in case the network requests it at a later stage.
			storage.WriteToOwnStateTransitionkStash(stateTransition)
			storage.ReceivedStateStash.Set(stateTransition.HashTransition(), stateTransition)
			logger.Printf("Broadcast block for height %d\n", currentBlock.Height)

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

	rootAcc := protocol.NewAccount(address, [32]byte{}, ActiveParameters.Staking_minimum, true, commPubKey, nil, nil)
	storage.State[addressHash] = &rootAcc
	storage.RootKeys[addressHash] = &rootAcc

	return nil
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
				//The following code makes sure that the newly staking node gets to mine the next epoch block
				if newStakingNode != [64]byte{} {
					logger.Printf("There is a new staking node")
					shardID := validatorShardAssignment[newStakingNode]
					logger.Printf("Designated ShardID of the new staking node: %d", shardID)
					//ned to fix the shard assignment
					if shardID != 1 {
						for designatedValidator, _ := range validatorShardAssignment {
							if validatorShardAssignment[designatedValidator] == 1 {
								logger.Printf("Validator with the designated shard ID 1: %x", designatedValidator[0:8])
								validatorShardAssignment[designatedValidator] = shardID
								validatorShardAssignment[newStakingNode] = 1
								break
							}
						}
					}
				} else {
					logger.Printf("Content of new staking node: %x", newStakingNode)
				}
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

	//The following code makes sure that the newly staking node gets to mine the next epoch block
	if newStakingNode != [64]byte{} {
		logger.Printf("There is a new staking node")
		shardID := validatorShardAssignment[newStakingNode]
		logger.Printf("Designated ShardID of the new staking node: %d", shardID)
		//ned to fix the shard assignment
		if shardID != 1 {
			for designatedValidator, _ := range validatorShardAssignment {
				if validatorShardAssignment[designatedValidator] == 1 {
					logger.Printf("Validator with the designated shard ID 1: %x", designatedValidator[0:8])
					validatorShardAssignment[designatedValidator] = shardID
					validatorShardAssignment[newStakingNode] = 1
					break
				}
			}
		}
	} else {
		logger.Printf("Content of new staking node: %x", newStakingNode)
	}
	return validatorShardAssignment
}


func searchStateTransition(shardID int, height int) *protocol.StateTransition {
	stateStash := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, uint32(height))
	for _,st := range stateStash {
		if st.ShardID == shardID {
			return st
		}
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
