package miner

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"github.com/oigele/bazo-miner/crypto"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"golang.org/x/crypto/blowfish"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	logger                       *log.Logger
	blockValidation              = &sync.Mutex{}
	parameterSlice               []Parameters
	ActiveParameters 			*Parameters
	uptodate                     bool
	slashingDict                 = make(map[[32]byte]SlashingProof)
	validatorAccAddress          [64]byte
	hasher                       [32]byte
	multisigPubKey               *ecdsa.PublicKey
	commPrivKey, rootCommPrivKey *rsa.PrivateKey
	// This map keeps track of the validator assignment to the shards
	ValidatorShardMap     *protocol.ValShardMapping
	ShardBlockMap 	      *protocol.ShardBlockMapping
	NumberOfShards    	  int
	NumberOfMinersInShard int
	// This slice stores the hashes of the last blocks from the other shards, needed to create the next epoch block.
	LastShardHashes [][32]byte

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

	firstValMapping := protocol.NewValShardMapping()
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


	//Listen for incoming blocks from the network
	go incomingData()
	//Listen for incoming epoch blocks from the network
	go incomingEpochData()
	//Listen for incoming state transitions the network
	go incomingStateData()
	//Listen for incoming block data transitions from the network
	go incomingBlockStateData()

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
		var validatorShardMapping = protocol.NewValShardMapping()
		var shardBlockMapping = protocol.NewShardBlockMapping()
		validatorShardMapping.ValMapping, shardBlockMapping.ShardBlockMapping = AssignValidatorsToShards()
		validatorShardMapping.EpochHeight = int(lastEpochBlock.Height)
		shardBlockMapping.EpochHeight = int(lastEpochBlock.Height)
		ValidatorShardMap = validatorShardMapping
		logger.Printf("Validator Shard Mapping:\n")
		logger.Printf(validatorShardMapping.String())
	}

	storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress]
	storage.ThisShardMap[int(lastEpochBlock.Height)] = storage.ThisShardID
	storage.ThisBlockMap[int(lastEpochBlock.Height)] = storage.ThisBlockID
	epochMining(lastBlock.Hash, lastBlock.Height)

	return nil
}

/**
Main function of Bazo which is running all the time with the goal of mining blocks and competing for the creation of epoch blocks.
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

		//Log the beginning of synchronisation
		logger.Printf("Before checking my state stash for lastblock height: %d\n", lastBlock.Height)
		syncStartTime = time.Now().Unix()

		//generate sequence of all shard IDs starting from 1
		shardIDs := makeRange(1, NumberOfShards)
		logger.Printf("Number of shards: %d\n", NumberOfShards)

		//This map keeps track of the shards whose state transitions have been processed.
		//Once all entries are set to true, the synchronisation is done and the validator can continue with mining of the next shard block
		shardIDStateBoolMap := make(map[int]bool)
		for k, _ := range shardIDStateBoolMap {
			shardIDStateBoolMap[k] = false
		}

		for {
			//If there is only one shard, then skip synchronisation mechanism
			if (NumberOfShards == 1) {
				break
			}

			logger.Printf("Shards this epoch: %d", NumberOfShardsDelayed)
			logger.Printf("My Shard ID this epoch %d", storage.ThisShardIDDelayed)
			//Retrieve all state transitions from the local state with the height of my last block
			stateStashForHeight := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash, lastBlock.Height)

			if (len(stateStashForHeight) != 0) {
				//Iterate through state transitions and apply them to local state, keep track of processed shards
				for _, st := range stateStashForHeight {
					if (shardIDStateBoolMap[st.ShardID] == false && st.ShardID != storage.ThisShardIDDelayed) {
						//Apply all relative account changes to my local state

						logger.Printf("Transactions to delete in this miner count: %d - New Mempool Size: %d\n",len(st.ContractTxData)+len(st.FundsTxData)+len(st.ConfigTxData)+ len(st.StakeTxData) + len(st.AggTxData),storage.GetMemPoolSize())

						storage.State = storage.ApplyRelativeState(storage.State, st.RelativeStateChange)

						//Delete transactions from Mempool (Transaction pool), which were validated
						//by the other shards to avoid starvation in the mempool
						DeleteTransactionFromMempool(st.ContractTxData, st.FundsTxData, st.ConfigTxData, st.StakeTxData, st.AggTxData)
						//Set the particular shard as being processed
						shardIDStateBoolMap[st.ShardID] = true

						logger.Printf("Processed state transition of shard: %d\n", st.ShardID)
					}
				}
				//If all state transitions have been received, stop synchronisation. Note that the state stash also includes the own transition
				if (len(stateStashForHeight) == NumberOfShardsDelayed) {
					logger.Printf("Already received all state transitions")
					break
				}
			}

			//Iterate over shard IDs to check which ones are still missing, and request them from the network
			for _, id := range shardIDs {
				if (id != storage.ThisShardIDDelayed && shardIDStateBoolMap[id] == false) {
					var stateTransition *protocol.StateTransition

					//it might be possible that a new validator started mining an epoch block too late. In this case, the bootstrap can broadcast the epoch block again
					//also, maybe a node got stuck. Help it
					//Quickfix. TODO: find a better mechanism for that
					broadcastEpochBlock(storage.ReadLastClosedEpochBlock())
					time.Sleep(time.Second)

					logger.Printf("requesting state transition for lastblock height: %d shard: %d\n", lastBlock.Height, id)

					p2p.StateTransitionReqShard(id, int(lastBlock.Height))
					//Blocking wait
					select {
					case encodedStateTransition := <-p2p.StateTransitionShardReqChan:
						stateTransition = stateTransition.DecodeStateTransition(encodedStateTransition)
						//weird check that has to be done (run into an infinite loop when I dint)
						if stateTransition.ShardID != id {
							logger.Printf("Error: Id of the iteration: %d --- Id of the received transition: %d", id, stateTransition.ShardID)
							continue
						}
						//Apply state transition to my local state
						storage.State = storage.ApplyRelativeState(storage.State, stateTransition.RelativeStateChange)

						logger.Printf("Writing state back to stash Shard ID: %v  VS my shard ID: %v - Height: %d\n", stateTransition.ShardID, storage.ThisShardID, stateTransition.Height)
						storage.ReceivedStateStash.Set(stateTransition.HashTransition(), stateTransition)

						//Delete transactions from mempool, which were validated by the other shards
						DeleteTransactionFromMempool(stateTransition.ContractTxData, stateTransition.FundsTxData, stateTransition.ConfigTxData, stateTransition.StakeTxData, stateTransition.AggTxData)

						shardIDStateBoolMap[stateTransition.ShardID] = true

						logger.Printf("Processed state transition of shard: %d\n", stateTransition.ShardID)


					case <-time.After(5 * time.Second):
						logger.Printf("have been waiting for 5 seconds for lastblock height: %d\n", lastBlock.Height)
						//It the requested state transition has not been received, then continue with requesting the other missing ones
						logger.Printf("broadcasting state transition from last block height %d again in case it couldnt be transmitted", int(lastBlock.Height) - 1)
						lastTransition := storage.ReadStateTransitionFromOwnStash(int(lastBlock.Height) - 1)
						//overwrite in case the previous block is an epoch block. then the last transition for that height is nil and we need to go further back
						if lastTransition == nil {
							lastTransition = storage.ReadStateTransitionFromOwnStash(int(lastBlock.Height) - 2)
						}
						if lastTransition != nil {
							broadcastStateTransition(lastTransition)
							time.Sleep(500 * time.Millisecond)
						}
						broadcastStateTransition(storage.ReadStateTransitionFromOwnStash(int(lastBlock.Height)))
						time.Sleep(time.Second)
						continue
					}
				}
			}
		}
		//Log the end of synchronisation
		logger.Printf("After checking my state stash for lastblock height: %d\n", lastBlock.Height)

		var syncEndTime = time.Now().Unix()
		var syncDuration = syncEndTime - syncStartTime
		totalSyncTime += syncDuration

		logger.Printf("Synchronisation duration for lastblock height: %d - %d seconds\n", lastBlock.Height, syncDuration)
		logger.Printf("Total Synchronisation duration for lastblock height: %d - %d seconds\n", lastBlock.Height, totalSyncTime)

		prevBlockIsEpochBlock = false

		// The variable 'lastblock' is one before the next epoch block, thus the next block will be an epoch block
		//really naive sortition: ShardID one can mint the epoch block
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
				err := finalizeEpochBlock(epochBlock)
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
						broadcastEpochBlock(storage.ReadLastClosedEpochBlock())
						epochBlockReceived = true
					}
				}
			}
			prevBlockIsEpochBlock = true
			firstEpochOver = true
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

			//TODO change this piece of code into the intra-shard comminication/sending schmeme
			//the intra-shard communications will be sent out here and there will be a blocking wait until it's finished
			//note that there is a possibility to accelerate the process. But this acceleration takes time to implement. Check notes.

			blockTransition := protocol.NewBlockTransition(storage.RelativeState, int(currentBlock.Height), storage.ThisShardID, storage.ThisBlockID, currentBlock.Hash,
				currentBlock.ContractTxData, currentBlock.FundsTxData, currentBlock.ConfigTxData, currentBlock.StakeTxData, currentBlock.AggTxData)

			broadcastBlockTransition(blockTransition)

			storage.WriteToOwnBlockTransitionkStash(blockTransition)
			storage.ReceivedBlockTransitionStash.Set(blockTransition.HashTransition(), blockTransition)

			

			//This node is the leader in the shard. It has to mint the shard block and send the transitions out to the network.
			if storage.ThisBlockID == 1 {


				currentShardBlock := protocol.NewShardBlock(currentBlock.Hash, currentBlock.Height + 1)
				currentShardBlock.ShardID = storage.ThisShardID
				currentShardBlock.
				//Generate state transition for this block. This data is needed by the other shards to update their local states.
				//use the freshly updated shardId, because the block always has to be in the new epoch already. If it was in the old epoch,
				//the epoch block would not have been generated either
				stateTransition := protocol.NewStateTransition(storage.RelativeState, int(currentBlock.Height), storage.ThisShardID, currentBlock.Hash,
					currentBlock.ContractTxData, currentBlock.FundsTxData, currentBlock.ConfigTxData, currentBlock.StakeTxData, currentBlock.AggTxData)

				logger.Printf("Transactions to delete in other miners count: %d - New Mempool Size: %d\n",len(stateTransition.ContractTxData)+len(stateTransition.FundsTxData)+len(stateTransition.ConfigTxData)+ len(stateTransition.StakeTxData) + len(stateTransition.AggTxData),storage.GetMemPoolSize())

				logger.Printf("Broadcast state transition for height %d\n", currentBlock.Height)
				//Broadcast state transition to other shards
				broadcastStateTransition(stateTransition)
				//Write state transition to own stash. Needed in case the network requests it at a later stage.
				storage.WriteToOwnStateTransitionkStash(stateTransition)
				storage.ReceivedStateStash.Set(stateTransition.HashTransition(), stateTransition)
			} else {
				//wait until shard block is received
				shardBlockReceived := false
				for !shardBlockReceived {
					newShardBlock := <- p2p.ShardBlockReceivedChan
					if newShardBlock.Height == lastBlock.Height + 1 {
						broadcastShardBlock(storage.ReadLastClosedShardBlock())
						shardBlockReceived = true
					}
				}
			}

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
func AssignValidatorsToShards() (map[[64]byte]int, map[[64]byte]int) {

	/*This map denotes which validator is assigned to which shard index*/
	validatorShardAssignment := make(map[[64]byte]int)
	nodeToBlockAssignment := make(map[[64]byte]int)

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

			if len(validatorSlices) == 0 {
				return validatorShardAssignment, nodeToBlockAssignment
			}

			randomIndex := rand.Intn(len(validatorSlices))
			randomValidator := validatorSlices[randomIndex]

			//Assign validator to shard ID
			validatorShardAssignment[randomValidator] = i

			//already got the random
			nodeToBlockAssignment[randomValidator] = j
			//Remove assigned validator from active list
			validatorSlices = removeValidator(validatorSlices, randomIndex)
		}
	}
	return validatorShardAssignment, nodeToBlockAssignment
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
