package miner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/oigele/bazo-miner/crypto"
	"sync"
	"time"

	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"golang.org/x/crypto/sha3"
)
var validateMutex = sync.Mutex{}
//Tests whether the first diff bits are zero
func validateProofOfStake(diff uint8,
	prevProofs [][crypto.COMM_PROOF_LENGTH]byte,
	height uint32,
	balance uint64,
	commitmentProof [crypto.COMM_PROOF_LENGTH]byte,
	timestamp int64) bool {

	validateMutex.Lock()
	defer validateMutex.Unlock()

	var (
		heightBuf    [4]byte
		timestampBuf [8]byte
		hashArgs []byte
	)

	// allocate memory
	// n * COMM_PROOF_LENGTH bytes (prevProofs) + COMM_PROOF_LENGTH bytes (commitmentProof)+ 4 bytes (height) + 8 bytes (count)
	hashArgs = make([]byte, len(prevProofs)*crypto.COMM_PROOF_LENGTH+crypto.COMM_PROOF_LENGTH+4+8)

	binary.BigEndian.PutUint32(heightBuf[:], height)
	binary.BigEndian.PutUint64(timestampBuf[:], uint64(timestamp))

	index := 0
	for _, prevProof := range prevProofs {
		copy(hashArgs[index:index + crypto.COMM_PROOF_LENGTH], prevProof[:])
		index += crypto.COMM_PROOF_LENGTH
	}

	copy(hashArgs[index:index + crypto.COMM_PROOF_LENGTH], commitmentProof[:]) // COMM_KEY_LENGTH bytes
	index += crypto.COMM_PROOF_LENGTH
	copy(hashArgs[index:index + 4], heightBuf[:]) 		// 4 bytes
	index += 4

	copy(hashArgs[index:index+8], timestampBuf[:])

	//calculate the hash
	pos := sha3.Sum256(hashArgs[:])

	data := binary.BigEndian.Uint64(pos[:])
	data = data / balance
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, data)

	copy(pos[0:32], buf.Bytes())

	var byteNr uint8
	//Bytes check
	for byteNr = 0; byteNr < (uint8)(diff/8); byteNr++ {
		if pos[byteNr] != 0 {
			return false
		}
	}
	//Bits check
	if diff%8 != 0 && pos[byteNr] >= 1<<(8-diff%8) {
		return false
	}
	return true
}

//diff and partialHash is needed to calculate a valid PoS, prevHash is needed to check whether we should stop
//PoS calculation because another block has been validated meanwhile
func proofOfStake(diff uint8,
	prevHash [32]byte,
	prevProofs [][crypto.COMM_PROOF_LENGTH]byte,
	height uint32,
	balance uint64,
	commitmentProof [crypto.COMM_PROOF_LENGTH]byte) (int64, error) {

	var (
		pos    [32]byte
		byteNr uint8
		abort  bool

		timestampBuf [8]byte
		heightBuf    [4]byte

		timestamp int64

		hashArgs []byte
	)

	// allocate memory
	// n * COMM_KEY_LENGTH bytes (prevProofs) + COMM_KEY_LENGTH bytes (localCommPubKey)+ 4 bytes (height) + 8 bytes (count)
	hashArgs = make([]byte, len(prevProofs)*crypto.COMM_PROOF_LENGTH+crypto.COMM_PROOF_LENGTH+4+8)

	binary.BigEndian.PutUint32(heightBuf[:], height)

	// all required parameters are concatenated in the following order:
	// ([PrevProofs] ⋅ CommitmentProof ⋅ CurrentBlockHeight ⋅ Seconds)
	index := 0
	for _, prevProof := range prevProofs {
		copy(hashArgs[index:index + crypto.COMM_PROOF_LENGTH], prevProof[:])
		index += crypto.COMM_PROOF_LENGTH
	}

	copy(hashArgs[index:index + crypto.COMM_PROOF_LENGTH], commitmentProof[:]) // COMM_KEY_LENGTH bytes
	index += crypto.COMM_PROOF_LENGTH
	copy(hashArgs[index:index + 4], heightBuf[:]) 		// 4 bytes
	index += 4

	timestampBufIndexStart := index
	timestampBufIndexEnd := index + 8

	cnt := 0
	for range time.Tick(time.Second) {
		// lastBlock is a global variable which points to the last block. This check makes sure we abort if another
		// block has been validated
		cnt = cnt + 1
		logger.Printf("Try Block with Time: %v and cnt: %v", time.Now().Format("030405"), cnt)

		//If 30 blocks should have been received, break
		if cnt >= 30 * BLOCK_INTERVAL {
			logger.Printf("Mined %v sec and no block validated...? --> Strange... ", 30*BLOCK_INTERVAL)
			return -1, errors.New("Abort mining, Mined too long")
		}

		if lastBlock == nil {
			lastBlock = storage.ReadLastClosedBlock()
		}
		if lastBlock == nil {
			return -1, errors.New("Abort mining, No Last Block Found")
		}
		if prevHash != lastBlock.Hash {
			//Error code -2 initiates that probably a aggTx Should be deleted from open storage.
			logger.Printf("Abort mining, another block has been successfully validated in the meantime --> LastBlock: %x", lastBlock.Hash[0:8])
			return -2, errors.New("Abort mining, another block has been successfully validated in the meantime:")
		}

		abort = false

		//add the number of seconds that have passed since the Unix epoch (00:00:00 UTC, 1 January 1970)
		timestamp = time.Now().Unix()
		binary.BigEndian.PutUint64(timestampBuf[:], uint64(timestamp))
		copy(hashArgs[timestampBufIndexStart:timestampBufIndexEnd], timestampBuf[:]) //8 bytes

		//calculate the hash
		pos = sha3.Sum256(hashArgs[:])

		//divide the hash by the balance (should not happen but possible in a testing environment)
		data := binary.BigEndian.Uint64(pos[:])
		if balance == 0 {
			return -1, errors.New("Zero division: Account owns 0 coins.")
		}
		data = data / balance
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, data)

		copy(pos[0:32], buf.Bytes())

		//Byte check
		for byteNr = 0; byteNr < (uint8)(diff/8); byteNr++ {
			if pos[byteNr] != 0 {
				//continue begins the next iteration of the innermost
				abort = true
				break
			}
		}

		if abort {
			continue
		}
		//Bit check
		if diff%8 != 0 && pos[byteNr] >= 1<<(8-diff%8) {
			continue
		}
		break
	}

	cnt = 0
	return timestamp, nil
}

func GetLatestProofs(n int, block *protocol.Block) (prevProofs [][crypto.COMM_PROOF_LENGTH]byte) {

	for block.Height > 0 && n > 0 {
		//try to read block from 'closedblocks' and 'closedblockswithouttx' bucket.
		closedBlock := storage.ReadClosedBlock(block.PrevHash)
		if closedBlock == nil {
			closedBlock = storage.ReadClosedBlockWithoutTx(block.PrevHashWithoutTx)
		}
		if closedBlock == nil {
			return
		}
		prevProofs = append(prevProofs, closedBlock.CommitmentProof)
		n -= 1
		block = closedBlock
	}
	return prevProofs
}
