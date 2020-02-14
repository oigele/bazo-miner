package storage

import (
	"crypto/rsa"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/oigele/bazo-miner/protocol"
)

var (
	db                 				*bolt.DB
	logger             				*log.Logger
	//don't get confused with the key of the account.
	State              				= make(map[[32]byte]*protocol.Account)
	//This map keeps track of the relative account adjustments within a shard, such as balance, txcount and stakingheight
	RelativeState                     = make(map[[32]byte]*protocol.RelativeAccount)
	OwnStateTransitionStash 		[]*protocol.StateTransition
	RootKeys           				= make(map[[32]byte]*protocol.Account)
	txMemPool          				= make(map[[32]byte]protocol.Transaction)

	//designed a new mempool for this use case
	AssignedTxMempool 				=  make(map[[32]byte]protocol.Transaction)

	//map of transaction assignments for a given height. Key: ShardID. Value: Assignment
	AssignedTxMap					= make(map[int]*protocol.TransactionAssignment)

	OwnCommitteeCheck 					 *protocol.CommitteeCheck

	openTxToDeleteMempool			= make(map[[32]byte]bool)
	txINVALIDMemPool   				= make(map[[32]byte]protocol.Transaction)
	bootstrapReceivedMemPool		= make(map[[32]byte]protocol.Transaction)
	DifferentSenders   				= make(map[[32]byte]uint32)
	DifferentSendersData			= make(map[[32]byte]uint32)
	DifferentReceivers				= make(map[[32]byte]uint32)
	FundsTxBeforeAggregation		= make([]*protocol.FundsTx, 0)
	DataTxBeforeAggregation			= make([]*protocol.DataTx, 0)
	ReceivedBlockStash				= make([]*protocol.Block, 0)
	TxcntToTxMap					= make(map[uint32][][32]byte)
	ValidatorAccAddress 			[64]byte


	AllClosedBlocksAsc []*protocol.Block
	Bootstrap_Server string
	averageTxSize float32 				= 0
	totalTransactionSize float32 		= 0
	nrClosedTransactions float32 		= 0
	openTxMutex 						= &sync.Mutex{}
	openTxToDeleteMutex					= &sync.Mutex{}
	openINVALIDTxMutex 					= &sync.Mutex{}
	assignedTransactionMutex			= &sync.Mutex{}
	openFundsTxBeforeAggregationMutex	= &sync.Mutex{}
	openDataTxBeforeAggregationMutex	= &sync.Mutex{}
	txcntToTxMapMutex					= &sync.Mutex{}
	ReceivedBlockStashMutex				= &sync.Mutex{}
	//Added by Kürsat
	ThisShardID             int // ID of the shard this validator is assigned to
	ThisShardIDDelayed		int
	ThisShardMap			= make(map[int]int)
	EpochLength				int
	ReceivedStateStash                      = protocol.NewStateStash()
	ReceivedShardBlockStash					= protocol.NewShardBlockStash()
	ReceivedCommitteeCheckStash				= protocol.NewCommitteeCheckStash()
	ReceivedTransactionAssignmentStash		= protocol.NewTransactionAssignmentStash()
	memPoolMutex                        = &sync.Mutex{}

	IsCommittee			bool
	AssignmentHeight	int

	CommitteeLeader [32]byte
	CommitteePrivKey                *rsa.PrivateKey
)

const (
	ERROR_MSG = "Initiate storage aborted: "
	CLOSEDEPOCHBLOCK_BUCKET = "closedepochblocks"
	LASTCLOSEDEPOCHBLOCK_BUCKET = "lastclosedepochblocks"
	OPENEPOCHBLOCK_BUCKET	= "openepochblock"
	GENESIS_BUCKET			= "genesis"
)

//Entry function for the storage package
func Init(dbname string, bootstrapIpport string) {
	Bootstrap_Server = bootstrapIpport
	logger = InitLogger()

	var err error
	db, err = bolt.Open(dbname, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		logger.Fatal(ERROR_MSG, err)
	}

	//Check if db file is empty for all non-bootstraping miners
	//if ipport != BOOTSTRAP_SERVER_PORT {
	//	err := db.View(func(tx *bolt.Tx) error {
	//		err := tx.ForEach(func(name []byte, bkt *bolt.Bucket) error {
	//			err := bkt.ForEach(func(k, v []byte) error {
	//				if k != nil && v != nil {
	//					return errors.New("Non-empty database given.")
	//				}
	//				return nil
	//			})
	//			return err
	//		})
	//		return err
	//	})
	//
	//	if err != nil {
	//		logger.Fatal(ERROR_MSG, err)
	//	}
	//}

	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("openblocks"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedblocks"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedblockswithouttx"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedfunds"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedaccs"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedstakes"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedcommittees"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedaggregations"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedconfigs"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closeddata"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("closedaggdata"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("lastclosedblock"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("genesis"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("openepochblock"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte(CLOSEDEPOCHBLOCK_BUCKET))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte(LASTCLOSEDEPOCHBLOCK_BUCKET))
		if err != nil {
			return fmt.Errorf(ERROR_MSG+"Create bucket: %s", err)
		}
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucket([]byte("datasummary"))
		if err != nil {
			return fmt.Errorf(ERROR_MSG + "Create bucket: %s", err)
		}
		return nil
	})
}

func TearDown() {
	db.Close()
}


