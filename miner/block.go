package miner

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/oigele/bazo-miner/crypto"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"github.com/oigele/bazo-miner/vm"
	"golang.org/x/crypto/sha3"
)

//Datastructure to fetch the payload of all transactions, needed for state validation.
type blockData struct {
	accTxSlice    		  		[]*protocol.AccTx
	fundsTxSlice  		  		[]*protocol.FundsTx
	configTxSlice 		  		[]*protocol.ConfigTx
	stakeTxSlice  		  		[]*protocol.StakeTx
	aggTxSlice	  		  		[]*protocol.AggTx
	aggregatedFundsTxSlice  	[]*protocol.FundsTx
	block        		  		*protocol.Block
}

/* TODO BUILD BACK IN
//Block constructor, argument is the previous block in the blockchain.
func newBlock(prevHash [32]byte, prevHashWithoutTx [32]byte, commitmentProof [crypto.COMM_PROOF_LENGTH]byte, height uint32) *protocol.Block {
	block := new(protocol.Block)
	block.PrevHash = prevHash
	block.PrevHashWithoutTx = prevHashWithoutTx
	block.CommitmentProof = commitmentProof
	block.Height = height
	block.StateCopy = make(map[[32]byte]*protocol.Account)
	block.Aggregated = false

	return block
}

 */

//TODO remove
func newBlock(prevHash [32]byte, commitmentProof [crypto.COMM_PROOF_LENGTH]byte, height uint32) *protocol.Block {
	block := new(protocol.Block)
	block.PrevHash = prevHash
	block.CommitmentProof = commitmentProof
	block.Height = height
	block.StateCopy = make(map[[32]byte]*protocol.Account)
	//Doesnt get the delayed shard id, because a newly mined block will always have the fresh shard assignment.
	block.ShardId = storage.ThisShardID

	return block
}

var (
	aggregationMutex = &sync.Mutex{}
	addFundsTxMutex = &sync.Mutex{}
)

//This function prepares the block to broadcast into the network. No new txs are added at this point.
func finalizeBlock(block *protocol.Block) error {
	//Check if we have a slashing proof that we can add to the block.
	//The slashingDict is updated when a new block is received and when a slashing proof is provided.
	logger.Printf("-- Start Finalize")
	if len(slashingDict) != 0 {
		//Get the first slashing proof.
		for hash, slashingProof := range slashingDict {
			block.SlashedAddress = hash
			block.ConflictingBlockHash1 = slashingProof.ConflictingBlockHash1
			block.ConflictingBlockHash2 = slashingProof.ConflictingBlockHash2
//			block.ConflictingBlockHashWithoutTx1 = slashingProof.ConflictingBlockHashWithoutTx1
//			block.ConflictingBlockHashWithoutTx2 = slashingProof.ConflictingBlockHashWithoutTx2
			break
		}
	}

	//Merkle tree includes the hashes of all txs in this block
	block.MerkleRoot = protocol.BuildMerkleTree(block).MerkleRoot()
	validatorAcc, err := storage.GetAccount(protocol.SerializeHashContent(validatorAccAddress))
	if err != nil {
		return err
	}

	validatorAccHash := validatorAcc.Hash()
	copy(block.Beneficiary[:], validatorAccHash[:])

	// Cryptographic Sortition for PoS in Bazo
	// The commitment proof stores a signed message of the Height that this block was created at.
	commitmentProof, err := crypto.SignMessageWithRSAKey(commPrivKey, fmt.Sprint(block.Height))
	if err != nil {
		return err
	}


	//Block hash with MerkleTree and therefore, including all transactions
	partialHash := block.HashBlock()

	//Block hash without MerkleTree and therefore, without any transactions
	//TODO uncomment
//	partialHashWithoutMerkleRoot := block.HashBlockWithoutMerkleRoot()

	prevProofs := GetLatestProofs(ActiveParameters.num_included_prev_proofs, block)
	nonce, err := proofOfStake(getDifficulty(), block.PrevHash, prevProofs, block.Height, validatorAcc.Balance, commitmentProof)
	if err != nil {
		//Delete all partially added transactions.
		if nonce == -2 {
			for _, tx := range storage.FundsTxBeforeAggregation {
				storage.WriteOpenTx(tx)
			}
			storage.DeleteAllFundsTxBeforeAggregation()
		}
		logger.Printf("We have an error in proof of stake in finalization")
		return err
	}

	var nonceBuf [8]byte
	binary.BigEndian.PutUint64(nonceBuf[:], uint64(nonce))
	block.Nonce = nonceBuf
	block.Timestamp = nonce

	//Put pieces together to get the final hash.
	block.Hash = sha3.Sum256(append(nonceBuf[:], partialHash[:]...))
	//TODO UNCOMMENT
//	block.HashWithoutTx = sha3.Sum256(append(nonceBuf[:], partialHashWithoutMerkleRoot[:]...))

	//This doesn't need to be hashed, because we already have the merkle tree taking care of consistency.
	block.NrAccTx = uint16(len(block.AccTxData))
	block.NrFundsTx = uint16(len(block.FundsTxData))
	logger.Printf("%d", len(block.FundsTxData))
	block.NrConfigTx = uint8(len(block.ConfigTxData))
	block.NrStakeTx = uint16(len(block.StakeTxData))
	block.NrAggTx = uint16(len(block.AggTxData))

	copy(block.CommitmentProof[0:crypto.COMM_PROOF_LENGTH], commitmentProof[:])
	logger.Printf("-- End Finalization")
	return nil
}

/**
Code from Kürsat
*/
func finalizeEpochBlock(epochBlock *protocol.EpochBlock) error {

	validatorAcc, err := storage.ReadAccount(validatorAccAddress)
	if err != nil {
		return err
	}


	// Cryptographic Sortition for PoS in Bazo
	// The commitment proof stores a signed message of the Height that this block was created at.
	commitmentProof, err := crypto.SignMessageWithRSAKey(commPrivKey, fmt.Sprint(epochBlock.Height))
	if err != nil {
		return err
	}

	partialHash := epochBlock.HashEpochBlock()

	/*Determine new number of shards needed based on current state*/
	NumberOfShards = DetNumberOfShards()

	//generate new validator mapping and include mappping in the epoch block
	valMapping := protocol.NewMapping()
	valMapping.ValMapping = AssignValidatorsToShards()
	valMapping.EpochHeight = int(epochBlock.Height)

	epochBlock.ValMapping = valMapping
	ValidatorShardMap = epochBlock.ValMapping
	epochBlock.NofShards = DetNumberOfShards()

	storage.ThisShardID = ValidatorShardMap.ValMapping[validatorAccAddress]
	storage.ThisShardMap[int(epochBlock.Height)] = storage.ThisShardID

	epochBlock.State = storage.State
	logger.Printf("Before Epoch Block proofofstake for height: %d\n",epochBlock.Height)

	nonce, err := proofOfStakeEpoch(getDifficulty(), lastEpochBlock.Hash, epochBlock.Height, validatorAcc.Balance, commitmentProof)
	if err != nil {
		return err
	}
	logger.Printf("After Epoch Block proofofstake for height: %d\n",epochBlock.Height)

	var nonceBuf [8]byte
	binary.BigEndian.PutUint64(nonceBuf[:], uint64(nonce))
	epochBlock.Timestamp = nonce

	//Put pieces together to get the final hash.
	epochBlock.Hash = sha3.Sum256(append(nonceBuf[:], partialHash[:]...))

	copy(epochBlock.CommitmentProof[0:crypto.COMM_PROOF_LENGTH], commitmentProof[:])

	return nil
}

//Transaction validation operates on a copy of a tiny subset of the state (all accounts involved in transactions).
//We do not operate global state because the work might get interrupted by receiving a block that needs validation
//which is done on the global state.
func addTx(b *protocol.Block, tx protocol.Transaction) error {
	//ActiveParameters is a datastructure that stores the current system parameters, gets only changed when
	//configTxs are broadcast in the network.

	//Switch this becasue aggtx fee is zero and otherwise this would lead to problems.
	switch tx.(type) {
	case *protocol.AggTx:
		return nil
	default :
		if tx.TxFee() < ActiveParameters.Fee_minimum {
			err := fmt.Sprintf("Transaction fee too low: %v (minimum is: %v)\n", tx.TxFee(), ActiveParameters.Fee_minimum)
			return errors.New(err)
		}
	}

	//There is a trade-off what tests can be made now and which have to be delayed (when dynamic state is needed
	//for inspection. The decision made is to check whether accTx and configTx have been signed with rootAcc. This
	//is a dynamic test because it needs to have access to the rootAcc state. The other option would be to include
	//the address (public key of signature) in the transaction inside the tx -> would resulted in bigger tx size.
	//So the trade-off is effectively clean abstraction vs. tx size. Everything related to fundsTx is postponed because
	//the txs depend on each other.
	if !verify(tx) {
		logger.Printf("Transaction could not be verified: %v", tx)
		return errors.New("Transaction could not be verified.")
	}

	switch tx.(type) {
	case *protocol.AccTx:
		err := addAccTx(b, tx.(*protocol.AccTx))
		if err != nil {
			logger.Printf("Adding accTx (%x) failed (%v): %v\n",tx.Hash(), err, tx.(*protocol.AccTx))

			return err
		}
	case *protocol.FundsTx:
		err := addFundsTx(b, tx.(*protocol.FundsTx))
		if err != nil {
			//logger.Printf("Adding fundsTx (%x) failed (%v): %v\n",tx.Hash(), err, tx.(*protocol.FundsTx))
			//logger.Printf("Adding fundsTx (%x) failed (%v)",tx.Hash(), err)
			return err
		}
	case *protocol.ConfigTx:
		err := addConfigTx(b, tx.(*protocol.ConfigTx))
		if err != nil {
			logger.Printf("Adding configTx (%x) failed (%v): %v\n",tx.Hash(), err, tx.(*protocol.ConfigTx))
			return err
		}
	case *protocol.StakeTx:
		err := addStakeTx(b, tx.(*protocol.StakeTx))
		if err != nil {
			logger.Printf("Adding stakeTx (%x) failed (%v): %v\n",tx.Hash(), err, tx.(*protocol.StakeTx))
			return err
		}
	default:
		return errors.New("Transaction type not recognized.")
	}
	return nil
}

func addAccTx(b *protocol.Block, tx *protocol.AccTx) error {
	accHash := protocol.SerializeHashContent(tx.PubKey)
	//According to the accTx specification, we only accept new accounts except if the removal bit is
	//set in the header (2nd bit).
	if tx.Header&0x02 != 0x02 {
		if _, exists := storage.State[accHash]; exists {
			return errors.New("Account already exists.")
		}
	}

	//Add the tx hash to the block header and write it to open storage (non-validated transactions).
	b.AccTxData = append(b.AccTxData, tx.Hash())
	logger.Printf("Added tx (%x) to the AccTxData slice: %v", tx.Hash(), *tx)
	return nil
}

func addFundsTx(b *protocol.Block, tx *protocol.FundsTx) error {
	addFundsTxMutex.Lock()

	//Checking if the sender account is already in the local state copy. If not and account exist, create local copy.
	//If account does not exist in state, abort.
	if _, exists := b.StateCopy[tx.From]; !exists {
		if acc := storage.State[tx.From]; acc != nil {
			hash := protocol.SerializeHashContent(acc.Address)
			if hash == tx.From {
				newAcc := protocol.Account{}
				newAcc = *acc
				b.StateCopy[tx.From] = &newAcc
			}
		} else {
			storage.WriteINVALIDOpenTx(tx)
			addFundsTxMutex.Unlock()
			logger.Printf("Sender account not present in the state: %x\n", tx.From)
			return errors.New(fmt.Sprintf("Sender account not present in the state: %x\n", tx.From))
		}
	}

	//Vice versa for receiver account.
	if _, exists := b.StateCopy[tx.To]; !exists {
		if acc := storage.State[tx.To]; acc != nil {
			hash := protocol.SerializeHashContent(acc.Address)
			if hash == tx.To {
				newAcc := protocol.Account{}
				newAcc = *acc
				b.StateCopy[tx.To] = &newAcc
			}
		} else {
			storage.WriteINVALIDOpenTx(tx)
			addFundsTxMutex.Unlock()
			return errors.New(fmt.Sprintf("Receiver account not present in the state: %x\n", tx.To))
		}
	}

	//Root accounts are exempt from balance requirements. All other accounts need to have (at least)
	//fee + amount to spend as balance available.
	if !storage.IsRootKey(tx.From) {
		if (tx.Amount + tx.Fee) > b.StateCopy[tx.From].Balance {
			storage.WriteINVALIDOpenTx(tx)
			addFundsTxMutex.Unlock()
			return errors.New("Not enough funds to complete the transaction!")
		}
	}

	//Transaction count need to match the state, preventing replay attacks.
	if b.StateCopy[tx.From].TxCnt != tx.TxCnt {
		if tx.TxCnt < b.StateCopy[tx.From].TxCnt {
			closedTx := storage.ReadClosedTx(tx.Hash())
			if closedTx != nil {
				storage.DeleteOpenTx(tx)
				storage.DeleteINVALIDOpenTx(tx)
				addFundsTxMutex.Unlock()
				return nil
			} else {
				addFundsTxMutex.Unlock()
				return nil
			}
		} else {
			storage.WriteINVALIDOpenTx(tx)
		}
		err := fmt.Sprintf("Sender %x txCnt does not match: %v (tx.txCnt) vs. %v (state txCnt)\nAggrgated: %t",tx.From, tx.TxCnt, b.StateCopy[tx.From].TxCnt, tx.Aggregated)
		storage.WriteINVALIDOpenTx(tx)
		addFundsTxMutex.Unlock()
		return errors.New(err)
	}

	//Prevent balance overflow in receiver account.
	if b.StateCopy[tx.To].Balance+tx.Amount > MAX_MONEY {
		err := fmt.Sprintf("Transaction amount (%v) leads to overflow at receiver account balance (%v).\n", tx.Amount, b.StateCopy[tx.To].Balance)
		storage.WriteINVALIDOpenTx(tx)
		addFundsTxMutex.Unlock()
		return errors.New(err)
	}

	//Check if transaction has data and the receiver account has a smart contract
	if tx.Data != nil && b.StateCopy[tx.To].Contract != nil {
		context := protocol.NewContext(*b.StateCopy[tx.To], *tx)
		virtualMachine := vm.NewVM(context)

		// Check if vm execution run without error
		if !virtualMachine.Exec(false) {
			storage.WriteINVALIDOpenTx(tx)
			addFundsTxMutex.Unlock()
			return errors.New(virtualMachine.GetErrorMsg())
		}

		//Update changes vm has made to the contract variables
		context.PersistChanges()
	}

	//Update state copy.
	accSender := b.StateCopy[tx.From]
	accSender.TxCnt += 1
	accSender.Balance = accSender.Balance - (tx.Amount + tx.Fee)

	accReceiver := b.StateCopy[tx.To]
	accReceiver.Balance += tx.Amount

	//Add teh transaction to the storage where all Funds-transactions are stored before they where aggregated.
	storage.WriteFundsTxBeforeAggregation(tx)

	addFundsTxMutex.Unlock()
	return nil
}

func addFundsTxFinal(b *protocol.Block, tx *protocol.FundsTx) error {
	b.FundsTxData = append(b.FundsTxData, tx.Hash())
	return nil
}

func addAggTxFinal(b *protocol.Block, tx *protocol.AggTx) error {
	b.AggTxData = append(b.AggTxData, tx.Hash())
	sort.Sort(ByHash(b.AggTxData))
	return nil
}

func splitSortedAggregatableTransactions(b *protocol.Block){
	//Explanation: Aggregates as many transactions as possible. Each time considering the local maximum
	txToAggregate := make([]protocol.Transaction, 0)
	moreTransactionsToAggregate := true

	PossibleTransactionsToAggregate := storage.ReadFundsTxBeforeAggregation()

	sortTxBeforeAggregation(PossibleTransactionsToAggregate)
	cnt := 0

	//The following code has been narrowed down to the use case for only aggregating according to senders. To see how the funcion was like originally check fabios github.
	for moreTransactionsToAggregate {

		//Get Sender which is most common
		_, addressSender := getMaxKeyAndValueFormMap(storage.DifferentSenders)

		// The sender which is most common is selected and all transactions are added to the txToAggregate
		// slice. The number of transactions sent/ will lower with every tx added. Then the splitted transactions
		// get aggregated into the correct aggregation transaction type and then written into the block.
		i:=0
		for _, tx := range PossibleTransactionsToAggregate {
			if tx.From == addressSender {
				txToAggregate = append(txToAggregate, tx)
			} else {
				PossibleTransactionsToAggregate[i] = tx
				i++
			}
		}

		PossibleTransactionsToAggregate = PossibleTransactionsToAggregate[:i]
		storage.DifferentSenders = map[[32]byte]uint32{}

		//Count senders and receivers again, because some transactions are removed now.
		for _, tx := range PossibleTransactionsToAggregate {
			storage.DifferentSenders[tx.From] = storage.DifferentSenders[tx.From] + 1
		}

		//Aggregate Transactions
		AggregateTransactions(txToAggregate, b)

		//Empty Slice
		txToAggregate = txToAggregate[:0]

		if len(PossibleTransactionsToAggregate) > 0 && len(storage.DifferentSenders) > 0  {
			if cnt > 20 {
				moreTransactionsToAggregate = false
			}
			cnt ++

			moreTransactionsToAggregate = true
		} else {
			cnt = 0
			moreTransactionsToAggregate = false
		}
	}

	storage.DeleteAllFundsTxBeforeAggregation()
}

func searchTransactionsInHistoricBlocks(searchAddressSender [32]byte, searchAddressReceiver [32]byte) (historicTransactions []protocol.Transaction) {

	for _, block := range storage.ReadAllClosedBlocksWithTransactions() {

		//Read all FundsTxIncluded in the block
		for _, txHash := range block.FundsTxData {
			tx := storage.ReadClosedTx(txHash)
			if tx != nil {
				trx := tx.(*protocol.FundsTx)
				if trx != nil && trx.Aggregated == false && (trx.From == searchAddressSender || trx.To == searchAddressReceiver) {
					historicTransactions = append(historicTransactions, trx)
					trx.Aggregated = true
					storage.WriteClosedTx(trx)
				}
			} else {
				return nil
			}
		}
		for _, txHash := range block.AggTxData {
			tx := storage.ReadClosedTx(txHash)
			if tx != nil {
				trx := tx.(*protocol.AggTx)
				if trx != nil && trx.Aggregated == false && (trx.From[0] == searchAddressSender || trx.To[0] == searchAddressReceiver) {
					logger.Printf("Found AggTx (%x) in (%x) which can be aggregated now.", trx.Hash(), block.Hash[0:8])
					historicTransactions = append(historicTransactions, trx)
					trx.Aggregated = true
					storage.WriteClosedTx(trx)
				}
			} else {
				//Tx Was not closes
				return nil
			}
		}
	}
	return historicTransactions
}

func getMaxKeyAndValueFormMap(m map[[32]byte]uint32) (uint32, [32]byte) {
	var max uint32 = 0
	biggestK := [32]byte{}
	for k := range m {
		if m[k] > max {
			max = m[k]
			biggestK = k
		}
	}

	return max, biggestK
}

//Here, I already made sure that TX will be aggregated according to sender. Maybe make code slimmer to improve performance
func AggregateTransactions(SortedAndSelectedFundsTx []protocol.Transaction, block *protocol.Block) error {
	aggregationMutex.Lock()
	defer aggregationMutex.Unlock()

	var transactionHashes, transactionReceivers, transactionSenders [][32]byte
	var nrOfSenders = map[[32]byte]int{}
	var nrOfReceivers = map[[32]byte]int{}
	var amount uint64

	//Sum up Amount, copy sender and receiver to correct slices and to map to check if aggregation by sender or receiver.
	for _, tx := range SortedAndSelectedFundsTx {

		trx := tx.(*protocol.FundsTx)
		amount += trx.Amount
		transactionSenders = append(transactionSenders, trx.From)
		nrOfSenders[trx.From] = nrOfSenders[trx.From] + 1
		transactionReceivers = append(transactionReceivers, trx.To)
		nrOfReceivers[trx.To] = nrOfReceivers[trx.To] + 1
		transactionHashes = append(transactionHashes, trx.Hash())
		storage.WriteOpenTx(trx)

	}

	//Check if only one sender or receiver is in the slices and if yes, shorten them.
	if len(nrOfSenders) == 1 {
		transactionSenders = transactionSenders[:1]
	}
	if len(nrOfReceivers) == 1 {
		transactionReceivers = transactionReceivers[:1]
	}

	/* The following block searches in historic transactions in historic blocks. However, this is not needed anymore
	// set addresses by which historic transactions should be searched.
	// If Zero-32byte array is sent, it will not find any addresses, because BAZOAccountAddresses
	if len(nrOfSenders) == len(nrOfReceivers){
		//Here transactions are searched to aggregate. if one is fund, it will aggregate accordingly.
		breakingForLoop := false
		for _, block := range storage.ReadAllClosedBlocksWithTransactions() {
			//Search All fundsTx to check if there are transactions with the same sender orrReceiver
			for _, fundsTxHash := range block.FundsTxData {
				trx := storage.ReadClosedTx(fundsTxHash)
				if len(transactionSenders) > 0 && trx.(*protocol.FundsTx).From == transactionSenders[0] {
					historicTransactions = searchTransactionsInHistoricBlocks(transactionSenders[0], [32]byte{})
					breakingForLoop = true
					break
				} else if len(transactionReceivers) > 0 && trx.(*protocol.FundsTx).To == transactionReceivers[0] {
					historicTransactions = searchTransactionsInHistoricBlocks([32]byte{}, transactionReceivers[0])
					breakingForLoop = true
					break
				}
			}
			if breakingForLoop == true {
				break
			}
			//Search all aggTx
			for _, aggTxHash := range block.AggTxData {
				trx := storage.ReadClosedTx(aggTxHash)
				if trx != nil {
					if len(trx.(*protocol.AggTx).From) == 1 && len(transactionSenders) > 0 && trx.(*protocol.AggTx).From[0] == transactionSenders[0] {
						historicTransactions = searchTransactionsInHistoricBlocks(transactionSenders[0], [32]byte{})
						breakingForLoop = true
						break
					} else if len(trx.(*protocol.AggTx).To) == 1 && len(transactionReceivers) > 0 && trx.(*protocol.AggTx).To[0] == transactionReceivers[0] {
						historicTransactions = searchTransactionsInHistoricBlocks([32]byte{}, transactionReceivers[0])
						breakingForLoop = true
						break
					}
				}
			}
			if breakingForLoop == true {
				break
			}
		}
	} else if len(nrOfSenders) < len(nrOfReceivers) {
		historicTransactions = searchTransactionsInHistoricBlocks(transactionSenders[0], [32]byte{})
	} else if len(nrOfSenders) > len(nrOfReceivers) {
		historicTransactions = searchTransactionsInHistoricBlocks([32]byte{}, transactionReceivers[0])
	}


	//Add transactions to the transactionsHashes slice
	for _, tx := range historicTransactions {
		transactionHashes = append(transactionHashes, tx.Hash())
	}
	*/
	if len(transactionHashes) > 1 {

		//Create Aggregated Transaction
		aggTx, err := protocol.ConstrAggTx(
			amount,
			0,
			transactionSenders,
			transactionReceivers,
			transactionHashes,
		)

		if err != nil {
			logger.Printf("%v\n", err)
			return err
		}

		//Print aggregated Transaction
		logger.Printf("%v", aggTx)

		//Add Aggregated transaction and write to open storage
		addAggTxFinal(block, aggTx)
		storage.WriteOpenTx(aggTx)

		//Sett all back to "zero"
		SortedAndSelectedFundsTx = nil
		amount = 0
		transactionReceivers = nil
		transactionHashes = nil

	} else if len(SortedAndSelectedFundsTx) > 0{
		addFundsTxFinal(block, SortedAndSelectedFundsTx[0].(*protocol.FundsTx))
	} else {
		err := errors.New("NullPointer")
		return err
	}

	return nil
}

// The next few functions below are used for sorting the List of transactions which can be aggregated.
// The Mempool is only sorted according to teh TxCount, So sorting the transactions which can be aggregated by sender
// and TxCount eases the aggregation process.
// It is implemented near to https://golang.org/pkg/sort/
type lessFunc func(p1, p2 *protocol.FundsTx) bool

type multiSorter struct {
	transactions []*protocol.FundsTx
	less    []lessFunc
}

func (ms *multiSorter) Sort(transactionsToSort []*protocol.FundsTx) {
	ms.transactions = transactionsToSort
	sort.Sort(ms)
}

func OrderedBy(less ...lessFunc) *multiSorter {
	return &multiSorter{
		less: less,
	}
}

func (ms *multiSorter) Len() int {
	return len(ms.transactions)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.transactions[i], ms.transactions[j] = ms.transactions[j], ms.transactions[i]
}

func (ms *multiSorter) Less(i, j int) bool {
	p, q := ms.transactions[i], ms.transactions[j]
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			return true
		case less(q, p):
			return false
		}
	}
	return ms.less[k](p, q)
}

func sortTxBeforeAggregation(Slice []*protocol.FundsTx) {
	//These Functions are inserted in the OrderBy function above. According to them the slice will be sorted.
	// It is sorted according to the Sender and the transactioncount.
	sender := func(c1, c2 *protocol.FundsTx) bool {
		return string(c1.From[:32]) < string(c2.From[:32])
	}
	txcount:= func(c1, c2 *protocol.FundsTx) bool {
		return c1.TxCnt< c2.TxCnt
	}

	OrderedBy(sender, txcount).Sort(Slice)
}

func addConfigTx(b *protocol.Block, tx *protocol.ConfigTx) error {
	//No further checks needed, static checks were already done with verify().
	b.ConfigTxData = append(b.ConfigTxData, tx.Hash())
	logger.Printf("Added tx (%x) to the ConfigTxData slice: %v", tx.Hash(), *tx)
	return nil
}

func addStakeTx(b *protocol.Block, tx *protocol.StakeTx) error {
	//Checking if the sender account is already in the local state copy. If not and account exist, create local copy
	//If account does not exist in state, abort.
	if _, exists := b.StateCopy[tx.Account]; !exists {
		if acc := storage.State[tx.Account]; acc != nil {
			hash := protocol.SerializeHashContent(acc.Address)
			if hash == tx.Account {
				newAcc := protocol.Account{}
				newAcc = *acc
				b.StateCopy[tx.Account] = &newAcc
			}
		} else {
			return errors.New(fmt.Sprintf("Sender account not present in the state: %x\n", tx.Account))
		}
	}

	//Root accounts are exempt from balance requirements. All other accounts need to have (at least)
	//fee + minimum amount that is required for staking.
	if !storage.IsRootKey(tx.Account) {
		if (tx.Fee + ActiveParameters.Staking_minimum) >= b.StateCopy[tx.Account].Balance {
			return errors.New("Not enough funds to complete the transaction!")
		}
	}

	//Account has bool already set to the desired value.
	if b.StateCopy[tx.Account].IsStaking == tx.IsStaking {
		return errors.New("Account has bool already set to the desired value.")
	}

	//Update state copy.
	accSender := b.StateCopy[tx.Account]
	accSender.IsStaking = tx.IsStaking
	accSender.CommitmentKey = tx.CommitmentKey

	//No further checks needed, static checks were already done with verify().
	b.StakeTxData = append(b.StakeTxData, tx.Hash())
	logger.Printf("Added tx (%x) to the StakeTxData slice: %v", tx.Hash(), *tx)
	return nil
}

//We use slices (not maps) because order is now important.
func fetchAccTxData(block *protocol.Block, accTxSlice []*protocol.AccTx, initialSetup bool, errChan chan error) {
	for cnt, txHash := range block.AccTxData {
		var tx protocol.Transaction
		var accTx *protocol.AccTx

		closedTx := storage.ReadClosedTx(txHash)
		if closedTx != nil {
			if initialSetup {
				accTx = closedTx.(*protocol.AccTx)
				accTxSlice[cnt] = accTx
				continue
			} else {
				//Reject blocks that have txs which have already been validated.
				errChan <- errors.New("Block validation had accTx that was already in a previous block.")
				return
			}
		}

		//TODO Optimize code (duplicated)
		//Tx is either in open storage or needs to be fetched from the network.
		tx = storage.ReadOpenTx(txHash)
		if tx != nil {
			accTx = tx.(*protocol.AccTx)
		} else {
			err := p2p.TxReq(txHash, p2p.ACCTX_REQ)
			if err != nil {
				errChan <- errors.New(fmt.Sprintf("AccTx could not be read: %v", err))
				return
			}

			//Blocking Wait
			select {
			case accTx = <-p2p.AccTxChan:
				//Limit the waiting time for TXFETCH_TIMEOUT seconds.
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				errChan <- errors.New("AccTx fetch timed out.")
			}
			//This check is important. A malicious miner might have sent us a tx whose hash is a different one
			//from what we requested.
			if accTx.Hash() != txHash {
				errChan <- errors.New("Received AcctxHash did not correspond to our request.")
			}
		}

		accTxSlice[cnt] = accTx
	}

	errChan <- nil
}

func fetchFundsTxData(block *protocol.Block, fundsTxSlice []*protocol.FundsTx, initialSetup bool, errChan chan error) {
	for cnt, txHash := range block.FundsTxData {
		var tx protocol.Transaction
		var fundsTx *protocol.FundsTx

		closedTx := storage.ReadClosedTx(txHash)
		if closedTx != nil {
			if initialSetup {
				fundsTx = closedTx.(*protocol.FundsTx)
				fundsTxSlice[cnt] = fundsTx
				continue
			} else {
				logger.Printf("Block validation had fundsTx (%x) that was already in a previous block.", closedTx.Hash())
				errChan <- errors.New("Block validation had fundsTx that was already in a previous block.")
				return
			}
		}

		//We check if the Transaction is in the invalidOpenTX stash. When it is in there, and it is valid now, we save
		//it into the fundsTX and continue like usual. This additional stash does lower the amount of network requests.
		tx = storage.ReadOpenTx(txHash)
		txINVALID := storage.ReadINVALIDOpenTx(txHash)
		if tx != nil {
			fundsTx = tx.(*protocol.FundsTx)
		} else if  txINVALID != nil && verify(txINVALID) {
			fundsTx = txINVALID.(*protocol.FundsTx)
		} else {
			err := p2p.TxReq(txHash, p2p.FUNDSTX_REQ)
			if err != nil {
				errChan <- errors.New(fmt.Sprintf("FundsTx could not be read: %v", err))
				return
			}
			select {
			case fundsTx = <-p2p.FundsTxChan:
				storage.WriteOpenTx(fundsTx)
				if initialSetup {
					storage.WriteBootstrapTxReceived(fundsTx)
				}
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				stash := p2p.ReceivedFundsTXStash
				if p2p.FundsTxAlreadyInStash(stash, txHash){
					for _, tx := range stash {
						if tx.Hash() == txHash {
							fundsTx = tx
							break
						}
					}
					break
				} else {
					errChan <- errors.New("FundsTx fetch timed out")
					return
				}
			}
			if fundsTx.Hash() != txHash {
				errChan <- errors.New("Received FundstxHash did not correspond to our request.")
			}
		}

		fundsTxSlice[cnt] = fundsTx
	}

	errChan <- nil
}

func fetchConfigTxData(block *protocol.Block, configTxSlice []*protocol.ConfigTx, initialSetup bool, errChan chan error) {
	for cnt, txHash := range block.ConfigTxData {
		var tx protocol.Transaction
		var configTx *protocol.ConfigTx

		closedTx := storage.ReadClosedTx(txHash)
		if closedTx != nil {
			if initialSetup {
				configTx = closedTx.(*protocol.ConfigTx)
				configTxSlice[cnt] = configTx
				continue
			} else {
				errChan <- errors.New("Block validation had configTx that was already in a previous block.")
				return
			}
		}

		//TODO Optimize code (duplicated)
		tx = storage.ReadOpenTx(txHash)
		if tx != nil {
			configTx = tx.(*protocol.ConfigTx)
		} else {
			err := p2p.TxReq(txHash, p2p.CONFIGTX_REQ)
			if err != nil {
				errChan <- errors.New(fmt.Sprintf("ConfigTx could not be read: %v", err))
				return
			}

			select {
			case configTx = <-p2p.ConfigTxChan:
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				errChan <- errors.New("ConfigTx fetch timed out.")
				return
			}
			if configTx.Hash() != txHash {
				errChan <- errors.New("Received ConfigtxHash did not correspond to our request.")
			}
		}

		configTxSlice[cnt] = configTx
	}

	errChan <- nil
}

func fetchStakeTxData(block *protocol.Block, stakeTxSlice []*protocol.StakeTx, initialSetup bool, errChan chan error) {
	for cnt, txHash := range block.StakeTxData {
		var tx protocol.Transaction
		var stakeTx *protocol.StakeTx

		closedTx := storage.ReadClosedTx(txHash)
		if closedTx != nil {
			if initialSetup {
				stakeTx = closedTx.(*protocol.StakeTx)
				stakeTxSlice[cnt] = stakeTx
				continue
			} else {
				errChan <- errors.New("Block validation had stakeTx that was already in a previous block.")
				return
			}
		}

		tx = storage.ReadOpenTx(txHash)
		if tx != nil {
			stakeTx = tx.(*protocol.StakeTx)
		} else {
			err := p2p.TxReq(txHash, p2p.STAKETX_REQ)
			if err != nil {
				errChan <- errors.New(fmt.Sprintf("StakeTx could not be read: %v", err))
				return
			}

			select {
			case stakeTx = <-p2p.StakeTxChan:
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				errChan <- errors.New("StakeTx fetch timed out.")
				return
			}
			if stakeTx.Hash() != txHash {
				errChan <- errors.New("Received StaketxHash did not correspond to our request.")
			}
		}

		stakeTxSlice[cnt] = stakeTx
	}

	errChan <- nil
}

//This function fetches the funds transactions recursively --> When a aggTx is agregated in another aggTx.
// This is mainly needed for the startup process. It is recursively searching until only funds transactions are in the list.
func fetchFundsTxRecursively(AggregatedTxSlice [][32]byte) (aggregatedFundsTxSlice []*protocol.FundsTx, err error){
	for _, txHash := range AggregatedTxSlice {
		//Try To read the transaction from closed storage.
		tx := storage.ReadClosedTx(txHash)

		if tx == nil {
			//Try to read it from open storage
			tx = storage.ReadOpenTx(txHash)
		}
		if tx == nil {
			//Read invalid storage when not found in closed & open Transactions
			tx = storage.ReadINVALIDOpenTx(txHash)
		}
		if tx == nil {
			//Fetch it from the network.
			err := p2p.TxReq(txHash, p2p.UNKNOWNTX_REQ)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("RECURSIVE Tx could not be read: %v", err))

			}

			//Depending on which channel the transaction is received, the type of the transaction is known.
			select {
			case tx = <-p2p.AggTxChan:
			case tx = <-p2p.FundsTxChan:
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				stash := p2p.ReceivedFundsTXStash
				aggTxStash := p2p.ReceivedAggTxStash

				if p2p.FundsTxAlreadyInStash(stash, txHash){
					for _, trx := range stash {
						if trx.Hash() == txHash {
							tx = trx
							break
						}
					}
					break
				} else if p2p.AggTxAlreadyInStash(aggTxStash, txHash){
					for _, trx := range stash {
						if trx.Hash() == txHash {
							tx = trx
							break
						}
					}
					break
				} else {
					logger.Printf("RECURSIVE Fetching (%x) timed out...", txHash)
					return nil, errors.New(fmt.Sprintf("RECURSIVE UnknownTx fetch timed out"))
				}
			}
			if tx.Hash() != txHash {
				return nil, errors.New(fmt.Sprintf("RECURSIVE Received TxHash did not correspond to our request."))
			}
			storage.WriteOpenTx(tx)
		}
		switch tx.(type) {
		case *protocol.FundsTx:
			aggregatedFundsTxSlice = append(aggregatedFundsTxSlice, tx.(*protocol.FundsTx))
		case *protocol.AggTx:
			//Do a recursive re-call for this function and append it to the Slice. Add temp just below
			temp, error := fetchFundsTxRecursively(tx.(*protocol.AggTx).AggregatedTxSlice)
			aggregatedFundsTxSlice = append(aggregatedFundsTxSlice, temp...)
			err = error

		}
	}

	if err == nil {
		return aggregatedFundsTxSlice, nil
	} else {
		return nil, err
	}

}

//This function fetches the AggTx's from a block. Furthermore it fetches missing transactions aggregated by these AggTx's
func fetchAggTxData(block *protocol.Block, aggTxSlice []*protocol.AggTx, initialSetup bool, errChan chan error, aggregatedFundsChan chan []*protocol.FundsTx) {
	var transactions []*protocol.FundsTx

	//First the aggTx is needed. Then the transactions aggregated in the AggTx
	for cnt, aggTxHash := range block.AggTxData {
		//Search transaction in closed transactions.
		aggTx := storage.ReadClosedTx(aggTxHash)

		//Check if transaction was already in another block, expect it is aggregated.
		if !initialSetup && aggTx != nil && aggTx.(*protocol.AggTx).Aggregated == false{
			if !aggTx.(*protocol.AggTx).Aggregated {
				logger.Printf("Block validation had AggTx (%x) that was already in a previous block.", aggTx.Hash())
				errChan <- errors.New("Block validation had AggTx that was already in a previous block.")
				return
			}
		}

		if aggTx == nil {
			//Read invalid storage when not found in closed & open Transactions
			aggTx = storage.ReadOpenTx(aggTxHash)
		}

		if aggTx == nil {
			//Read open storage when not found in closedTransactions
			aggTx = storage.ReadINVALIDOpenTx(aggTxHash)
		}
		if aggTx == nil {
			//Transaction need to be fetched from the network.
			cnt := 0
			HERE:
			logger.Printf("Request AGGTX: %x for block %x", aggTxHash, block.Hash[0:8])
			err := p2p.TxReq(aggTxHash, p2p.AGGTX_REQ)
			if err != nil {
				errChan <- errors.New(fmt.Sprintf("AggTx could not be read: %v", err))
				return
			}

			select {
			case aggTx = <-p2p.AggTxChan:
				storage.WriteOpenTx(aggTx)
				logger.Printf("  Received AGGTX: %x for block %x", aggTxHash, block.Hash[0:8])
			case <-time.After(TXFETCH_TIMEOUT * time.Second):
				stash := p2p.ReceivedAggTxStash
				if p2p.AggTxAlreadyInStash(stash, aggTxHash){
					for _, tx := range stash {
						if tx.Hash() == aggTxHash {
							aggTx = tx
							logger.Printf("  FOUND: Request AGGTX: %x for block %x in received Stash during timeout", aggTx.Hash(), block.Hash[0:8])
							break
						}
					}
					break
				}
				if cnt < 2 {
					cnt ++
					goto HERE
				}
				logger.Printf("  TIME OUT: Request AGGTX: %x for block %x", aggTxHash, block.Hash[0:8])
				errChan <- errors.New("AggTx fetch timed out")
				return
			}
			if aggTx.Hash() != aggTxHash {
				errChan <- errors.New("Received AggTxHash did not correspond to our request.")
			}
			logger.Printf("Received requested AggTX %x", aggTx.Hash())
		}

		//At this point the aggTx visible in the blocks body should be received.
		if aggTx == nil{
			errChan <- errors.New("AggTx Could not be found ")
			return
		}

		//Add Transaction to the aggTxSlice of the block.
		aggTxSlice[cnt] = aggTx.(*protocol.AggTx)

		//Now all transactions aggregated in the specific aggTx are handled. This are either new FundsTx which are
		//needed for the state update or other aggTx again aggregated. The later ones are validated in an older block.
		if initialSetup {
			//If this AggTx is Aggregated, all funds will be fetched later... --> take teh next AggTx.
			if aggTx.(*protocol.AggTx).Aggregated == true {
				continue
			}

			//All FundsTransactions are needed. Fetch them recursively. If an error occurs --> return.
			var err error
			transactions, err = fetchFundsTxRecursively(aggTx.(*protocol.AggTx).AggregatedTxSlice)
			if err != nil {
				errChan <- err
				return
			}
		} else {
			//Not all funds transactions are needed. Only the new ones. The other ones are already validated in the state.
			for _, txHash := range aggTx.(*protocol.AggTx).AggregatedTxSlice {
				tx := storage.ReadClosedTx(txHash)

				if tx != nil {
					//Found already closed transaction --> Not needed for further process.
					logger.Printf("Found Transaction %x which was in previous block.", tx.Hash())
					continue
				} else {
					tx = storage.ReadOpenTx(txHash)

					if tx != nil {
						//Found Open new Agg Transaction
						switch tx.(type) {
						case *protocol.AggTx:
							continue
						}
						transactions = append(transactions, tx.(*protocol.FundsTx))
					} else {
						if tx != nil {
							tx = storage.ReadINVALIDOpenTx(txHash)
							switch tx.(type) {
							case *protocol.AggTx:
								continue
							}
							transactions = append(transactions, tx.(*protocol.FundsTx))
						} else {
							//Need to fetch transaction from the network. At this point it is unknown what type of tx we request.
							cnt := 0
							NEXTTRY:
							err := p2p.TxReq(txHash, p2p.UNKNOWNTX_REQ)
							if err != nil {
								errChan <- errors.New(fmt.Sprintf("Tx could not be read: %v", err))
								return
							}

							//Depending on which channel the transaction is received, the type of the transaction is known.
							select {
							case tx = <-p2p.AggTxChan:
								//Received an Aggregated Transaction which was already validated in an older block.
								storage.WriteOpenTx(tx)
							case tx = <-p2p.FundsTxChan:
								//Received a fundsTransaction, which needs to be handled further.
								storage.WriteOpenTx(tx)
								transactions = append(transactions, tx.(*protocol.FundsTx))
							case <-time.After(TXFETCH_TIMEOUT * time.Second):
								aggTxStash := p2p.ReceivedAggTxStash
								if p2p.AggTxAlreadyInStash(aggTxStash, txHash){
									for _, trx := range aggTxStash {
										if trx.Hash() == txHash {
											tx = trx
											storage.WriteOpenTx(tx)
											break
										}
									}
									break
								}
								fundsTxStash := p2p.ReceivedFundsTXStash
								if p2p.FundsTxAlreadyInStash(fundsTxStash, txHash){
									for _, trx := range fundsTxStash {
										if trx.Hash() == txHash {
											tx = trx
											storage.WriteOpenTx(tx)
											transactions = append(transactions, tx.(*protocol.FundsTx))
											break
										}
									}
									break
								}
								if cnt < 2 {
									cnt ++
									goto NEXTTRY
								}
								logger.Printf("Fetching UnknownTX %x timed out for block %x", txHash, block.Hash[0:8])
								errChan <- errors.New("UnknownTx fetch timed out")
								return
							}
							if tx.Hash() != txHash {
								errChan <- errors.New("Received TxHash did not correspond to our request.")
							}
						}
					}
				}
			}
		}
	}

	//Send the transactions into the channel, otherwise send nil such that a deadlock cannot occur.
	if len(transactions) > 0 {
		aggregatedFundsChan <- transactions
	} else {
		aggregatedFundsChan <- nil
	}

	errChan <- nil
}

//This function is split into block syntax/PoS check and actual state change
//because there is the case that we might need to go fetch several blocks
// and have to check the blocks first before changing the state in the correct order.
func validate(b *protocol.Block, initialSetup bool) error {

	//This mutex is necessary that own-mined blocks and received blocks from the network are not
	//validated concurrently.
	blockValidation.Lock()
	defer blockValidation.Unlock()

	if storage.ReadClosedBlock(b.Hash) != nil {
		logger.Printf("Received block (%x) has already been validated.\n", b.Hash[0:8])
		return errors.New("Received Block has already been validated.")
	}

	//Prepare datastructure to fill tx payloads.
	blockDataMap := make(map[[32]byte]blockData)

	//Get the right branch, and a list of blocks to rollback (if necessary).
	blocksToRollback, blocksToValidate, err := getBlockSequences(b)
	if err != nil {
		return err
	}

	if len(blocksToRollback) > 0 {
		logger.Printf(" _____________________")
		logger.Printf("| Blocks To Rollback: |________________________________________________")
		for _, block := range blocksToRollback {
			logger.Printf("|  - %x  |", block.Hash)
		}
		logger.Printf("|______________________________________________________________________|")
		logger.Printf(" _____________________")
		logger.Printf("| Blocks To Validate: |________________________________________________")
		for _, block := range blocksToValidate {
			logger.Printf("|  - %x  |", block.Hash)
		}
		logger.Printf("|______________________________________________________________________|")
	}

	//Verify block time is dynamic and corresponds to system time at the time of retrieval.
	//If we are syncing or far behind, we cannot do this dynamic check,
	//therefore we include a boolean uptodate. If it's true we consider ourselves uptodate and
	//do dynamic time checking.
	if len(blocksToValidate) > DELAYED_BLOCKS {
		uptodate = false
	} else {
		uptodate = true
	}

	//No rollback needed, just a new block to validate.
	if len(blocksToRollback) == 0 {
		for i, block := range blocksToValidate {
			//Fetching payload data from the txs (if necessary, ask other miners).
			accTxs, fundsTxs, configTxs, stakeTxs, aggTxs, aggregatedFundsTxSlice, err := preValidate(block, initialSetup)

			//Check if the validator that added the block has previously voted on different competing chains (find slashing proof).
			//The proof will be stored in the global slashing dictionary.
			if block.Height > 0 {
				seekSlashingProof(block)
			}

			if err != nil {
				return err
			}

			blockDataMap[block.Hash] = blockData{accTxs, fundsTxs, configTxs, stakeTxs, aggTxs, aggregatedFundsTxSlice,block}

			var previousStateCopy = CopyState(storage.State)
			if err := validateState(blockDataMap[block.Hash], initialSetup); err != nil {
				return err
			}


			storage.RelativeState = storage.GetRelativeState(previousStateCopy,storage.State)

			postValidate(blockDataMap[block.Hash], initialSetup)
			if i != len(blocksToValidate)-1 {
				logger.Printf("Validated block (During Validation of other block %v): %vState:\n%v", b.Hash[0:8] , block, getState())
			}
		}
	} else {
		//Rollback
		for _, block := range blocksToRollback {
			if err := rollback(block); err != nil {
				return err
			}
		}

		//Validation of new chain
		for _, block := range blocksToValidate {
			//Fetching payload data from the txs (if necessary, ask other miners).
			accTxs, fundsTxs, configTxs, stakeTxs, aggTxs, aggregatedFundsTxSlice, err := preValidate(block, initialSetup)

			//Check if the validator that added the block has previously voted on different competing chains (find slashing proof).
			//The proof will be stored in the global slashing dictionary.
			if block.Height > 0 {
				seekSlashingProof(block)
			}

			if err != nil {
				return err
			}

			blockDataMap[block.Hash] = blockData{accTxs, fundsTxs, configTxs, stakeTxs, aggTxs, aggregatedFundsTxSlice,block}
			var previousStateCopy = CopyState(storage.State)
			if err := validateState(blockDataMap[block.Hash], initialSetup); err != nil {
				return err
			}


			storage.RelativeState = storage.GetRelativeState(previousStateCopy,storage.State)

			postValidate(blockDataMap[block.Hash], initialSetup)
			//logger.Printf("Validated block (after rollback): %x", block.Hash[0:8])
			logger.Printf("Validated block (after rollback for block %v): %vState:\n%v", b.Hash[0:8], block, getState())
		}
	}

	return nil
}

func CopyState(state map[[32]byte]*protocol.Account) map[[32]byte]protocol.Account {

	var copyState = make(map[[32]byte]protocol.Account)

	for k, v := range state {
		copyState[k] = *v
	}

	return copyState
}

//Doesn't involve any state changes.
func preValidate(block *protocol.Block, initialSetup bool) (accTxSlice []*protocol.AccTx, fundsTxSlice []*protocol.FundsTx, configTxSlice []*protocol.ConfigTx, stakeTxSlice []*protocol.StakeTx, aggTxSlice []*protocol.AggTx, aggregatedFundsTxSlice []*protocol.FundsTx, err error) {
	//This dynamic check is only done if we're up-to-date with syncing, otherwise timestamp is not checked.
	//Other miners (which are up-to-date) made sure that this is correct.
	if !initialSetup && uptodate {
		if err := timestampCheck(block.Timestamp); err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}
	}

	//Check block size.
	if block.GetSize() > ActiveParameters.Block_size {
		return nil, nil, nil, nil, nil, nil, errors.New("Block size too large.")
	}

	//Duplicates are not allowed, use tx hash hashmap to easily check for duplicates.
	duplicates := make(map[[32]byte]bool)
	for _, txHash := range block.AccTxData {
		if _, exists := duplicates[txHash]; exists {
			return nil, nil, nil, nil,  nil, nil, errors.New("Duplicate Account Transaction Hash detected.")
		}
		duplicates[txHash] = true
	}
	for _, txHash := range block.FundsTxData {
		if _, exists := duplicates[txHash]; exists {
			return nil, nil, nil, nil, nil, nil, errors.New("Duplicate Funds Transaction Hash detected.")
		}
		duplicates[txHash] = true
	}
	for _, txHash := range block.ConfigTxData {
		if _, exists := duplicates[txHash]; exists {
			return nil, nil, nil, nil, nil, nil, errors.New("Duplicate Config Transaction Hash detected.")
		}
		duplicates[txHash] = true
	}
	for _, txHash := range block.StakeTxData {
		if _, exists := duplicates[txHash]; exists {
			return nil, nil, nil, nil, nil, nil, errors.New("Duplicate Stake Transaction Hash detected.")
		}
		duplicates[txHash] = true
	}

	for _, txHash := range block.AggTxData {
		if _, exists := duplicates[txHash]; exists {
			return nil, nil, nil, nil, nil, nil, errors.New("Duplicate Aggregation Transaction Hash detected.")
		}
		duplicates[txHash] = true
	}

	//We fetch tx data for each type in parallel -> performance boost.
	nrOfChannels := 5
	errChan := make(chan error, nrOfChannels)
	aggregatedFundsChan := make(chan []*protocol.FundsTx, 10000)

	//We need to allocate slice space for the underlying array when we pass them as reference.
	accTxSlice = make([]*protocol.AccTx, block.NrAccTx)
	fundsTxSlice = make([]*protocol.FundsTx, block.NrFundsTx)
	configTxSlice = make([]*protocol.ConfigTx, block.NrConfigTx)
	stakeTxSlice = make([]*protocol.StakeTx, block.NrStakeTx)
	aggTxSlice = make([]*protocol.AggTx, block.NrAggTx)

	go fetchAccTxData(block, accTxSlice, initialSetup, errChan)
	go fetchFundsTxData(block, fundsTxSlice, initialSetup, errChan)
	go fetchConfigTxData(block, configTxSlice, initialSetup, errChan)
	go fetchStakeTxData(block, stakeTxSlice, initialSetup, errChan)
	go fetchAggTxData(block, aggTxSlice, initialSetup, errChan, aggregatedFundsChan)

	//Wait for all goroutines to finish.
	for cnt := 0; cnt < nrOfChannels; cnt++ {
		err = <-errChan
		if err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}
	}

	if len(aggTxSlice) > 0{
		logger.Printf("-- Fetch AggTxData - Start")
		select {
		case aggregatedFundsTxSlice = <- aggregatedFundsChan:
		case <-time.After(10 * time.Minute):
			return nil, nil, nil, nil, nil, nil, errors.New("Fetching FundsTx aggregated in AggTx failed.")
		}
		logger.Printf("-- Fetch AggTxData - End")
	}

	//Check state contains beneficiary.
	acc, err := storage.GetAccount(block.Beneficiary)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	//Check if node is part of the validator set.
	if !acc.IsStaking {
		return nil, nil, nil, nil, nil, nil, errors.New("Validator is not part of the validator set.")
	}

	//First, initialize an RSA Public Key instance with the modulus of the proposer of the block (acc)
	//Second, check if the commitment proof of the proposed block can be verified with the public key
	//Invalid if the commitment proof can not be verified with the public key of the proposer
	commitmentPubKey, err := crypto.CreateRSAPubKeyFromBytes(acc.CommitmentKey)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, errors.New("Invalid commitment key in account.")
	}

	err = crypto.VerifyMessageWithRSAKey(commitmentPubKey, fmt.Sprint(block.Height), block.CommitmentProof)
	logger.Printf("CommitmentPubKey: %x, --------------- Block Height: %d", commitmentPubKey, block.Height)
	if err != nil {
		return nil, nil, nil, nil, nil, nil,  errors.New("The submitted commitment proof can not be verified.")
	}

	//Invalid if PoS calculation is not correct.
	prevProofs := GetLatestProofs(ActiveParameters.num_included_prev_proofs, block)

	//PoS validation
	if !initialSetup && !validateProofOfStake(getDifficulty(), prevProofs, block.Height, acc.Balance, block.CommitmentProof, block.Timestamp) {
		logger.Printf("____________________NONCE (%x) in block %x is problematic", block.Nonce, block.Hash[0:8])
		logger.Printf("|  block.Height: %d, acc.Address %x, acc.txCount %v, acc.Balance %v, block.CommitmentProf: %x, block.Timestamp %v ", block.Height, acc.Address[0:8], acc.TxCnt,  acc.Balance, block.CommitmentProof[0:8], block.Timestamp)
		logger.Printf("|_____________________________________________________")

		return nil, nil, nil, nil, nil, nil, errors.New("The nonce is incorrect.")
	}

	//Invalid if PoS is too far in the future.
	now := time.Now()
	if block.Timestamp > now.Unix()+int64(ActiveParameters.Accepted_time_diff) {
		return nil, nil, nil, nil, nil, nil, errors.New("The timestamp is too far in the future. " + string(block.Timestamp) + " vs " + string(now.Unix()))
	}

	//Check for minimum waiting time.
	if block.Height-acc.StakingBlockHeight < uint32(ActiveParameters.Waiting_minimum) {
		return nil, nil, nil, nil, nil, nil, errors.New("The miner must wait a minimum amount of blocks before start validating. Block Height:" + fmt.Sprint(block.Height) + " - Height when started validating " + string(acc.StakingBlockHeight) + " MinWaitingTime: " + string(ActiveParameters.Waiting_minimum))
	}

	//Check if block contains a proof for two conflicting block hashes, else no proof provided.
	if block.SlashedAddress != [32]byte{} {
		if _, err = slashingCheck(block.SlashedAddress, block.ConflictingBlockHash1, block.ConflictingBlockHash2); err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}
	}

	//Merkle Tree validation
	// TODO build block.Aggregated == false && back in
	if  protocol.BuildMerkleTree(block).MerkleRoot() != block.MerkleRoot {
		return nil, nil, nil, nil, nil, nil, errors.New("Merkle Root is incorrect.")
	}

	return accTxSlice, fundsTxSlice, configTxSlice, stakeTxSlice, aggTxSlice, aggregatedFundsTxSlice, err
}

//Dynamic state check.
//TODO UNCOMMENT
func validateState(data blockData, initialSetup bool) error {
	//The sequence of validation matters. If we start with accs, then fund/stake transactions can be done in the same block
	//even though the accounts did not exist before the block validation.

	if err := accStateChange(data.accTxSlice); err != nil {
		return err
	}

	if err := fundsStateChange(data.fundsTxSlice, initialSetup); err != nil {
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	if err := aggTxStateChange(data.aggregatedFundsTxSlice, initialSetup); err != nil {
		fundsStateChangeRollback(data.fundsTxSlice)
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	if err := stakeStateChange(data.stakeTxSlice, data.block.Height, initialSetup); err != nil {
		fundsStateChangeRollback(data.fundsTxSlice)
		accStateChangeRollback(data.accTxSlice)
//		aggregatedStateRollback(data.aggTxSlice, data.block.HashWithoutTx, data.block.Beneficiary)
		return err
	}

	if err := collectTxFees(data.accTxSlice, data.fundsTxSlice, data.configTxSlice, data.stakeTxSlice, data.aggTxSlice, data.block.Beneficiary, initialSetup); err != nil {
		stakeStateChangeRollback(data.stakeTxSlice)
		fundsStateChangeRollback(data.fundsTxSlice)
//		aggregatedStateRollback(data.aggTxSlice, data.block.HashWithoutTx, data.block.Beneficiary)
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	if err := collectBlockReward(ActiveParameters.Block_reward, data.block.Beneficiary, initialSetup); err != nil {
		collectTxFeesRollback(data.accTxSlice, data.fundsTxSlice, data.configTxSlice, data.stakeTxSlice, data.block.Beneficiary)
		stakeStateChangeRollback(data.stakeTxSlice)
		fundsStateChangeRollback(data.fundsTxSlice)
//		aggregatedStateRollback(data.aggTxSlice, data.block.HashWithoutTx, data.block.Beneficiary)
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	if err := collectSlashReward(ActiveParameters.Slash_reward, data.block); err != nil {
		collectBlockRewardRollback(ActiveParameters.Block_reward, data.block.Beneficiary)
		collectTxFeesRollback(data.accTxSlice, data.fundsTxSlice, data.configTxSlice, data.stakeTxSlice, data.block.Beneficiary)
		stakeStateChangeRollback(data.stakeTxSlice)
		fundsStateChangeRollback(data.fundsTxSlice)
//		aggregatedStateRollback(data.aggTxSlice, data.block.HashWithoutTx, data.block.Beneficiary)
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	if err := updateStakingHeight(data.block); err != nil {
		collectSlashRewardRollback(ActiveParameters.Slash_reward, data.block)
		collectBlockRewardRollback(ActiveParameters.Block_reward, data.block.Beneficiary)
		collectTxFeesRollback(data.accTxSlice, data.fundsTxSlice, data.configTxSlice, data.stakeTxSlice, data.block.Beneficiary)
		stakeStateChangeRollback(data.stakeTxSlice)
		fundsStateChangeRollback(data.fundsTxSlice)
//		aggregatedStateRollback(data.aggTxSlice, data.block.HashWithoutTx, data.block.Beneficiary)
		accStateChangeRollback(data.accTxSlice)
		return err
	}

	return nil
}

func postValidate(data blockData, initialSetup bool) {

	//The new system parameters get active if the block was successfully validated
	//This is done after state validation (in contrast to accTx/fundsTx).
	//Conversely, if blocks are rolled back, the system parameters are changed first.
	configStateChange(data.configTxSlice, data.block.Hash)
	//Collects meta information about the block (and handled difficulty adaption).
	collectStatistics(data.block)

	//When starting a miner there are various scenarios how to PostValidate a block
	// 1. Bootstrapping Miner on InitialSetup 		--> All Tx Are already in closedBucket
	// 2. Bootstrapping Miner after InitialSetup	--> PostValidate normal, writing tx into closed bucket.
	// 3. Normal Miner on InitialSetup 				-->	Write All Tx Into Closed Tx
	// 4. Normal Miner after InitialSetup			-->	Write All Tx Into Closed Tx
	if !p2p.IsBootstrap() || !initialSetup {
		//Write all open transactions to closed/validated storage.
		for _, tx := range data.accTxSlice {
			storage.WriteClosedTx(tx)
			storage.DeleteOpenTx(tx)
		}

		for _, tx := range data.fundsTxSlice {
			storage.WriteClosedTx(tx)
			//TODO UNCOMMENT
//			tx.Block = data.block.HashWithoutTx
			storage.DeleteOpenTx(tx)
			storage.DeleteINVALIDOpenTx(tx)
		}

		for _, tx := range data.configTxSlice {
			storage.WriteClosedTx(tx)
			storage.DeleteOpenTx(tx)
		}

		for _, tx := range data.stakeTxSlice {
			storage.WriteClosedTx(tx)
			storage.DeleteOpenTx(tx)
		}

		//Store all recursively fetched funds transactions.
		if initialSetup {
			for _, tx := range data.aggregatedFundsTxSlice {
				tx.Aggregated = true
				storage.WriteClosedTx(tx)
				storage.DeleteOpenTx(tx)
			}
		}

		for _, tx := range data.aggTxSlice {

			//delete FundsTx per aggTx in open storage and write them to the closed storage.
			for _, aggregatedTxHash := range tx.AggregatedTxSlice {
				trx := storage.ReadClosedTx(aggregatedTxHash)
				if trx != nil {
					switch trx.(type){
					case *protocol.AggTx:
						trx.(*protocol.AggTx).Aggregated = true
					case *protocol.FundsTx:
						trx.(*protocol.FundsTx).Aggregated = true
					}
				} else {
					trx = storage.ReadOpenTx(aggregatedTxHash)
					if trx == nil {
						for _, i := range data.aggregatedFundsTxSlice {
							if i.Hash() == aggregatedTxHash {
								trx = i
							}
						}
					}
					switch trx.(type){
					case *protocol.AggTx:
						trx.(*protocol.AggTx).Aggregated = true
					case *protocol.FundsTx:
//						trx.(*protocol.FundsTx).Block = data.block.HashWithoutTx
						trx.(*protocol.FundsTx).Aggregated = true
					}
				}
				if trx == nil {
					break
				}

				storage.WriteClosedTx(trx)
				storage.DeleteOpenTx(trx)
				storage.DeleteINVALIDOpenTx(tx)
			}

			//Delete AggTx and write it to closed Tx.
			//TODO UNCOMMENT
//			tx.Block = data.block.HashWithoutTx
			tx.Aggregated = false
			storage.WriteClosedTx(tx)
			storage.DeleteOpenTx(tx)
			storage.DeleteINVALIDOpenTx(tx)
		}

		if len(data.fundsTxSlice) > 0 {
			broadcastVerifiedFundsTxs(data.fundsTxSlice)
			//Current sending mechanism is not  fast enough to broadcast all validated transactions...
			//broadcastVerifiedFundsTxsToOtherMiners(data.fundsTxSlice)
			//broadcastVerifiedFundsTxsToOtherMiners(data.aggregatedFundsTxSlice)
		}

		//Broadcast AggTx to the neighbors, such that they do not have to request them later.
		if len(data.aggTxSlice) > 0 {
			//broadcastVerifiedAggTxsToOtherMiners(data.aggTxSlice)
		}

		//It might be that block is not in the openblock storage, but this doesn't matter.
		storage.DeleteOpenBlock(data.block.Hash)
		storage.WriteClosedBlock(data.block)
		//logger.Printf("Inside Validation for block %x --> Inside Postvalidation (13)", data.block.Hash)

		//Do not empty last three blocks and only if it not aggregated already.
		/* TODO uncomment this loop
		for _, block := range storage.ReadAllClosedBlocks(){

			//Empty all blocks despite the last NO_AGGREGATION_LENGTH and genesis block.
			if !block.Aggregated && block.Height > 0 {
				if (int(block.Height)) < (int(data.block.Height) - NO_EMPTYING_LENGTH) {
					storage.UpdateBlocksToBlocksWithoutTx(block)
				}
			}
		}
		 */

		// Write last block to db and delete last block's ancestor.
		storage.DeleteAllLastClosedBlock()
		storage.WriteLastClosedBlock(data.block)
	}
}

//Only blocks with timestamp not diverging from system time (past or future) more than one hour are accepted.
func timestampCheck(timestamp int64) error {
	systemTime := p2p.ReadSystemTime()

	if timestamp > systemTime {
		if timestamp-systemTime > int64(2 * time.Hour.Seconds()) {
			return errors.New("Timestamp was too far in the future.System time: " + strconv.FormatInt(systemTime, 10) + " vs. timestamp " + strconv.FormatInt(timestamp, 10) + "\n")
		}
	} else {
		if systemTime-timestamp > int64(10 * time.Hour.Seconds()) {
			return errors.New("Timestamp was too far in the past. System time: " + strconv.FormatInt(systemTime, 10) + " vs. timestamp " + strconv.FormatInt(timestamp, 10) + "\n")
		}
	}

	return nil
}


//TODO connect with project
func slashingCheck(slashedAddress, conflictingBlockHash1, conflictingBlockHash2 [32]byte) (bool, error) {
	prefix := "Invalid slashing proof: "

	if conflictingBlockHash1 == [32]byte{} || conflictingBlockHash2 == [32]byte{} {
		return false, errors.New(fmt.Sprintf(prefix + "Invalid conflicting block hashes provided."))
	}

	if conflictingBlockHash1 == conflictingBlockHash2 {
		return false, errors.New(fmt.Sprintf(prefix + "Conflicting block hashes are the same."))
	}

	//Fetch the blocks for the provided block hashes.
	conflictingBlock1 := storage.ReadClosedBlock(conflictingBlockHash1)
	conflictingBlock2 := storage.ReadClosedBlock(conflictingBlockHash2)


	if IsInSameChain(conflictingBlock1, conflictingBlock2) {
		return false, errors.New(fmt.Sprintf(prefix + "Conflicting block hashes are on the same chain."))
	}

	//TODO Optimize code (duplicated)
	//If this block is unknown we need to check if its in the openblock storage or we must request it.
	if conflictingBlock1 == nil {
		conflictingBlock1 = storage.ReadOpenBlock(conflictingBlockHash1)
		if conflictingBlock1 == nil {
			//Fetch the block we apparently missed from the network.
			p2p.BlockReq(conflictingBlockHash1, conflictingBlockHash1)

			//Blocking wait
			select {
			case encodedBlock := <-p2p.BlockReqChan:
				conflictingBlock1 = conflictingBlock1.Decode(encodedBlock)
				//Limit waiting time to BLOCKFETCH_TIMEOUT seconds before aborting.
			case <-time.After(BLOCKFETCH_TIMEOUT * time.Second):
				if p2p.BlockAlreadyReceived(storage.ReadReceivedBlockStash(), conflictingBlockHash1) {
					for _, block := range storage.ReadReceivedBlockStash() {
						if block.Hash == conflictingBlockHash1 {
							conflictingBlock1 = block
							break
						}
					}
					logger.Printf("Block %x received Before", conflictingBlockHash1)
					break
				}
				return false, errors.New(fmt.Sprintf(prefix + "Could not find a block with the provided conflicting hash (1)."))
			}
		}

		ancestor, _ := getNewChain(conflictingBlock1)
		if ancestor == [32]byte{} {
			return false, errors.New(fmt.Sprintf(prefix + "Could not find a ancestor for the provided conflicting hash (1)."))
		}
	}

	//TODO Optimize code (duplicated)
	//If this block is unknown we need to check if its in the openblock storage or we must request it.
	if conflictingBlock2 == nil {
		conflictingBlock2 = storage.ReadOpenBlock(conflictingBlockHash2)
		if conflictingBlock2 == nil {
			//Fetch the block we apparently missed from the network.
			p2p.BlockReq(conflictingBlockHash2, conflictingBlockHash2)

			//Blocking wait
			select {
			case encodedBlock := <-p2p.BlockReqChan:
				conflictingBlock2 = conflictingBlock2.Decode(encodedBlock)
				//Limit waiting time to BLOCKFETCH_TIMEOUT seconds before aborting.
			case <-time.After(BLOCKFETCH_TIMEOUT * time.Second):
				if p2p.BlockAlreadyReceived(storage.ReadReceivedBlockStash(), conflictingBlockHash2) {
					for _, block := range storage.ReadReceivedBlockStash() {
						if block.Hash == conflictingBlockHash2 {
							conflictingBlock2 = block
							break
						}
					}
					logger.Printf("Block %x received Before", conflictingBlockHash2)
					break
				}
				return false, errors.New(fmt.Sprintf(prefix + "Could not find a block with the provided conflicting hash (2)."))
			}
		}

		ancestorHash, _ := getNewChain(conflictingBlock2)
		if ancestorHash == [32]byte{} {
			return false, errors.New(fmt.Sprintf(prefix + "Could not find a ancestor for the provided conflicting hash (2)."))
		}
	}

	// We found the height of the blocks and the height of the blocks can be checked.
	// If the height is not within the active slashing window size, we must throw an error. If not, the proof is valid.
	if !(conflictingBlock1.Height < uint32(ActiveParameters.Slashing_window_size)+conflictingBlock2.Height) {
		return false, errors.New(fmt.Sprintf(prefix + "Could not find a ancestor for the provided conflicting hash (2)."))
	}

	//Delete the proof from local slashing dictionary. If proof has not existed yet, nothing will be deleted.
	delete(slashingDict, slashedAddress)

	return true, nil
}