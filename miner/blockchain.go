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
	"sync"
	"time"
)

var (
	logger                       *log.Logger
	blockValidation              = &sync.Mutex{}
	parameterSlice               []Parameters
	activeParameters             *Parameters
	uptodate                     bool
	slashingDict                 = make(map[[32]byte]SlashingProof)
	validatorAccAddress          [64]byte
	multisigPubKey               *ecdsa.PublicKey
	commPrivKey, rootCommPrivKey *rsa.PrivateKey
	// This map keeps track of the validator assignment to the shards
	ValidatorShardMap       	 *protocol.ValShardMapping
	NumberOfShards          	  int
	// This slice stores the hashes of the last blocks from the other shards, needed to create the next epoch block.
	LastShardHashes         	  [][32]byte

	//Kursat Extras
	prevBlockIsEpochBlock   bool
	FirstStartAfterEpoch	bool
	blockStartTime			int64
	syncStartTime			int64
	blockEndTime			int64
	totalSyncTime			int64

)

//Miner entry point
func Init(validatorWallet, multisigWallet, rootWallet *ecdsa.PublicKey, validatorCommitment, rootCommitment *rsa.PrivateKey) {
	var err error


	validatorAccAddress = crypto.GetAddressFromPubKey(validatorWallet)
	multisigPubKey = multisigWallet
	commPrivKey = validatorCommitment
	rootCommPrivKey = rootCommitment

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

	logger.Printf("\n\n\n-------------------- START MINER ---------------------")
	logger.Printf("This Miners IP-Address: %v\n\n", p2p.Ipport)
	time.Sleep(2*time.Second)
	parameterSlice = append(parameterSlice, NewDefaultParameters())
	activeParameters = &parameterSlice[0]

	//Initialize root key.
	initRootKey(rootWallet)
	if err != nil {
		logger.Printf("Could not create a root account.\n")
	}

	currentTargetTime = new(timerange)
	target = append(target, 13)

	initialBlock, err := initState()
	if err != nil {
		logger.Printf("Could not set up initial state: %v.\n", err)
		return
	}

	logger.Printf("ActiveConfigParams: \n%v\n------------------------------------------------------------------------\n\nBAZO is Running\n\n", activeParameters)

	//this is used to generate the state with aggregated transactions.
	//TODO is this still necessary after the local state of the sharding is introduced?
	for _, tx := range storage.ReadAllBootstrapReceivedTransactions() {
		if tx != nil {
			storage.DeleteOpenTx(tx)
			storage.WriteClosedTx(tx)
		}
	}
	storage.DeleteBootstrapReceivedMempool()

	//Listen for incoming blocks from the network
	go incomingData()
	//Listen for incoming epoch blocks from the network
	go incomingEpochData()
	//Listen for incoming state transitions the network
	go incomingStateData()

	//Since new validators only join after the currently running epoch ends, they do no need to download the whole shardchain history,
	//but can continue with their work after the next epoch block and directly set their state to the global state of the first received epoch block
	if(p2p.IsBootstrap()){
		initialBlock, err = initState() //From here on, every validator should have the same state representation
		if err != nil {
			return
			//Return err ??
		}
		lastBlock = initialBlock
	} else {
		for{
			//As the non-bootstrapping node, wait until I receive the last epoch block as well as the validator assignment
			// The global variables 'lastEpochBlock' and 'ValidatorShardMap' are being set when they are received by the network
			if(lastEpochBlock != nil && ValidatorShardMap != nil){
				if(lastEpochBlock.Height > 0){
					storage.State = lastEpochBlock.State
					NumberOfShards = lastEpochBlock.NofShards
					storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress] //Save my ShardID
					FirstStartAfterEpoch = true

					lastBlock = dummyLastBlock
					epochMining(lastEpochBlock.Hash,lastEpochBlock.Height) //start mining based on the received Epoch Block
				}
			}
		}
	}
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
			mining(hashPrevBlock,heightPrevBlock)
		}

		//Log the beginning of synchronisation
		logger.Printf("Before checking my state stash for lastblock height: %d\n",lastBlock.Height)
		syncStartTime = time.Now().Unix()

		//generate sequence of all shard IDs starting from 1
		shardIDs := makeRange(1,NumberOfShards)
		logger.Printf("Number of shards: %d\n",NumberOfShards)

		//This map keeps track of the shards whose state transitions have been processed.
		//Once all entries are set to true, the synchronisation is done and the validator can continue with mining of the next shard block
		shardIDStateBoolMap := make(map[int]bool)
		for k, _ := range shardIDStateBoolMap {
			shardIDStateBoolMap[k] = false
		}

		for{
			//If there is only one shard, then skip synchronisation mechanism
			if(NumberOfShards == 1){
				break
			}

			//Retrieve all state transitions from the local state with the height of my last block
			stateStashForHeight := protocol.ReturnStateTransitionForHeight(storage.ReceivedStateStash,lastBlock.Height)

			if(len(stateStashForHeight) != 0){
				//Iterate through state transitions and apply them to local state, keep track of processed shards
				for _,st := range stateStashForHeight{
					if(shardIDStateBoolMap[st.ShardID] == false){
						//Apply all relative account changes to my local state
						storage.State = storage.ApplyRelativeState(storage.State,st.RelativeStateChange)
						//Delete transactions from Mempool (Transaction pool), which were validated
						//by the other shards to avoid starvation in the mempool
						DeleteTransactionFromMempool(st.ContractTxData,st.FundsTxData,st.ConfigTxData,st.StakeTxData)
						//Set the particular shard as being processed
						shardIDStateBoolMap[st.ShardID] = true

						logger.Printf("Processed state transition of shard: %d\n",st.ShardID)
					}
				}
				//If all state transitions have been received, stop synchronisation
				if (len(stateStashForHeight) == NumberOfShards-1){
					break
				}
			}

			//Iterate over shard IDs to check which ones are still missing, and request them from the network
			for _,id := range shardIDs{
				if(id != storage.ThisShardID && shardIDStateBoolMap[id] == false){
					var stateTransition *protocol.StateTransition

					logger.Printf("requesting state transition for lastblock height: %d\n",lastBlock.Height)

					p2p.StateTransitionReqShard(id,int(lastBlock.Height))
					//Blocking wait
					select {
					case encodedStateTransition := <-p2p.StateTransitionShardReqChan:
						stateTransition = stateTransition.DecodeTransition(encodedStateTransition)
						//Apply state transition to my local state
						storage.State = storage.ApplyRelativeState(storage.State,stateTransition.RelativeStateChange)

						logger.Printf("Writing state back to stash Shard ID: %v  VS my shard ID: %v - Height: %d\n",stateTransition.ShardID,storage.ThisShardID,stateTransition.Height)
						storage.ReceivedStateStash.Set(stateTransition.HashTransition(),stateTransition)

						//Delete transactions from mempool, which were validated by the other shards
						DeleteTransactionFromMempool(stateTransition.ContractTxData,stateTransition.FundsTxData,stateTransition.ConfigTxData,stateTransition.StakeTxData)

						shardIDStateBoolMap[stateTransition.ShardID] = true

						logger.Printf("Processed state transition of shard: %d\n",stateTransition.ShardID)

						//Limit waiting time to 5 seconds seconds before aborting.
					case <-time.After(5 * time.Second):
						logger.Printf("have been waiting for 5 seconds for lastblock height: %d\n",lastBlock.Height)
						//It the requested state transition has not been received, then continue with requesting the other missing ones
						continue
					}
				}
			}
		}
		//Log the end of synchronisation
		logger.Printf("After checking my state stash for lastblock height: %d\n",lastBlock.Height)

		var syncEndTime = time.Now().Unix()
		var syncDuration = syncEndTime - syncStartTime
		totalSyncTime += syncDuration

		logger.Printf("Synchronisation duration for lastblock height: %d - %d seconds\n",lastBlock.Height,syncDuration)
		logger.Printf("Total Synchronisation duration for lastblock height: %d - %d seconds\n",lastBlock.Height,totalSyncTime)

		prevBlockIsEpochBlock = false

		// The variable 'lastblock' is one before the next epoch block, thus the next block will be an epoch block
		if (lastBlock.Height == uint32(lastEpochBlock.Height) + uint32(activeParameters.epoch_length)) {
			epochBlock = protocol.NewEpochBlock([][32]byte{lastBlock.Hash}, lastBlock.Height+1)
			logger.Printf("epochblock beingprocessed height: %d\n",epochBlock.Height)


			if(NumberOfShards != 1){
				//Extract the hashes of the last blocks of the other shards, needed to create the epoch block
				//The hashes of the blocks are stored in the state transitions of the other shards
				LastShardHashes = protocol.ReturnShardHashesForHeight(storage.ReceivedStateStash,lastBlock.Height)
				epochBlock.PrevShardHashes = append(epochBlock.PrevShardHashes,LastShardHashes...)
			}

			logger.Printf("Before finalizeEpochBlock() ---- Height: %d\n",epochBlock.Height)
			//Finalize creation of the epoch block. In case another epoch block was mined in the meantime, abort PoS here
			err := finalizeEpochBlock(epochBlock)
			logger.Printf("After finalizeEpochBlock() ---- Height: %d\n",epochBlock.Height)

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
				logger.Printf(ValidatorShardMap.String()+"\n")
				logger.Printf("Inserting EPOCH BLOCK: %v\n", epochBlock.String())

				for _, prevHash := range epochBlock.PrevShardHashes {
					//FileConnections.WriteString(fmt.Sprintf("'%x' -> 'EPOCH BLOCK: %x'\n", prevHash[0:15], epochBlock.Hash[0:15]))
					logger.Printf(`"Hash : %x \n Height : %d" -> "EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+"\n", prevHash[0:8],epochBlock.Height-1,epochBlock.Hash[0:8],epochBlock.Height,epochBlock.MerklePatriciaRoot[0:8])
					logger.Printf(`"EPOCH BLOCK: \n Hash : %x \n Height : %d \nMPT : %x"`+`[color = red, shape = box]`+"\n",epochBlock.Hash[0:8],epochBlock.Height,epochBlock.MerklePatriciaRoot[0:8])
				}
			}

			//Introduce some delay in case there was a fork of the epoch block.
			//Even though the states of both epoch blocks are the same, the validator-shard assignment is likely to be different
			//General rule: Accept the last received epoch block as the valid one.
			time.Sleep(5*time.Second)

			prevBlockIsEpochBlock = true
			firstEpochOver = true
			//Continue mining with the hash of the last epoch block
			mining(lastEpochBlock.Hash, lastEpochBlock.Height)
		} else if(lastEpochBlock.Height == lastBlock.Height+1){
			prevBlockIsEpochBlock = true
			mining(lastEpochBlock.Hash, lastEpochBlock.Height) //lastblock was received before we started creation of next epoch block
		} else {
			mining(lastBlock.Hash, lastBlock.Height)
		}
	}
}

//Mining is a constant process, trying to come up with a successful PoW.
func mining(initialBlock *protocol.Block) {
	currentBlock := newBlock(initialBlock.Hash, initialBlock.HashWithoutTx, [crypto.COMM_PROOF_LENGTH]byte{}, initialBlock.Height+1)

	for {
		err := finalizeBlock(currentBlock)
		if err != nil {
			logger.Printf("%v\n", err)
		} else {
			logger.Printf("Block mined (%x)\n", currentBlock.Hash[0:8])
		}

		if err == nil {
			err := validate(currentBlock, false)
			if err == nil {
				//Only broadcast the block if it is valid.
				go broadcastBlock(currentBlock)
				logger.Printf("Validated block (mined): %vState:\n%v", currentBlock, getState())
			} else {
				logger.Printf("Mined block (%x) could not be validated: %v\n", currentBlock.Hash[0:8], err)
			}
		}

		//Prints miner connections
		p2p.EmptyingiplistChan()
		p2p.PrintMinerConns()



		//This is the same mutex that is claimed at the beginning of a block validation. The reason we do this is
		//that before start mining a new block we empty the mempool which contains tx data that is likely to be
		//validated with block validation, so we wait in order to not work on tx data that is already validated
		//when we finish the block.
		logger.Printf("\n\n __________________________________________________ New Mining Round __________________________________________________")
		blockValidation.Lock()
		logger.Printf("Create Next Block")
		nextBlock := newBlock(lastBlock.Hash, lastBlock.HashWithoutTx, [crypto.COMM_PROOF_LENGTH]byte{}, lastBlock.Height+1)
		currentBlock = nextBlock
		logger.Printf("Prepare Next Block")
		prepareBlock(currentBlock)
		logger.Printf("Prepare Next Block --> Done")
		blockValidation.Unlock()
	}
}

//At least one root key needs to be set which is allowed to create new accounts.
func initRootKey(rootKey *ecdsa.PublicKey) error {
	address := crypto.GetAddressFromPubKey(rootKey)
	addressHash := protocol.SerializeHashContent(address)

	var commPubKey [crypto.COMM_KEY_LENGTH]byte
	copy(commPubKey[:], rootCommPrivKey.N.Bytes())

	rootAcc := protocol.NewAccount(address, [32]byte{}, activeParameters.Staking_minimum, true, commPubKey, nil, nil)
	storage.State[addressHash] = &rootAcc
	storage.RootKeys[addressHash] = &rootAcc

	return nil
}

//Helper functions from Kürsat
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
