package p2p

import (
	"github.com/oigele/bazo-miner/storage"
	"log"
)

var (
	LogMapping map[uint8]string
	logger     *log.Logger
)

func InitLogging() {
	logger = storage.InitLogger()

	//Instead of logging just the integer, we log the corresponding semantic meaning, makes scrolling through
	//the log file more comfortable
	LogMapping = make(map[uint8]string)
	LogMapping[1]  = "FUNDSTX_BRDCST"
	LogMapping[2]  = "ACCTX_BRDCST"
	LogMapping[3]  = "CONFIGTX_BRDCST"
	LogMapping[4]  = "STAKETX_BRDCST"
	LogMapping[5]  = "VERIFIEDTX_BRDCST"
	LogMapping[6]  = "BLOCK_BRDCST"
	LogMapping[7]  = "BLOCK_HEADER_BRDCST"
	LogMapping[8]  = "TX_BRDCST_ACK"
	LogMapping[9]  = "AGGTX_BRDCST"

	LogMapping[19] = "GENESIS_REQ"
	LogMapping[20] = "FUNDSTX_REQ"
	LogMapping[21] = "ACCTX_REQ"
	LogMapping[22] = "CONFIGTX_REQ"
	LogMapping[23] = "STAKETX_REQ"
	LogMapping[24] = "BLOCK_REQ"
	LogMapping[25] = "BLOCK_HEADER_REQ"
	LogMapping[26] = "ACC_REQ"
	LogMapping[27] = "ROOTACC_REQ"
	LogMapping[28] = "INTERMEDIATE_NODES_REQ"
	LogMapping[29] = "AGGTX_REQ"
	LogMapping[30] = "UNKNOWNTX_REQ"
	LogMapping[31] = "SPECIALTX_REQ"
	LogMapping[32] = "NOT_FOUND_TX_REQ"

	LogMapping[40] = "FUNDSTX_RES"
	LogMapping[41] = "ACCTX_RES"
	LogMapping[42] = "CONFIGTX_RES"
	LogMapping[43] = "STAKETX_RES"
	LogMapping[44] = "BlOCK_RES"
	LogMapping[45] = "BlOCK_HEADER_RES"
	LogMapping[46] = "ACC_RES"
	LogMapping[47] = "ROOTACC_RES"
	LogMapping[48] = "INTERMEDIATE_NODES_RES"
	LogMapping[49] = "AGGTX_RES"

	LogMapping[130] = "NEIGHBOR_REQ"
	LogMapping[140] = "NEIGHBOR_RES"

	LogMapping[150] = "TIME_BRDCST"

	LogMapping[100] = "MINER_PING"
	LogMapping[101] = "MINER_PONG"
	LogMapping[102] = "CLIENT_PING"
	LogMapping[103] = "CLIENT_PONG"

	LogMapping[110] = "NOT_FOUND"

	LogMapping[120] = "STATE_REQ"
	LogMapping[121] = "STATE_RES"
	LogMapping[122] = "FIRST_EPOCH_BLOCK_REQ"
	LogMapping[123] = "FIRST_EPOCH_BLOCK_RES"
	LogMapping[124] = "EPOCH_BLOCK_REQ"
	LogMapping[125] = "EPOCH_BLOCK_RES"
	LogMapping[126] = "VALIDATOR_SHARD_BRDCST"
	LogMapping[127] = "VALIDATOR_SHARD_REQ"
	LogMapping[128] = "VALIDATOR_SHARD_RES"
	LogMapping[129] = "EPOCH_BLOCK_BRDCST"
	LogMapping[131] = "LAST_EPOCH_BLOCK_RES"
	LogMapping[132] = "LAST_EPOCH_BLOCK_REQ"
	LogMapping[133] = "STATE_TRANSITION_BRDCST"
	LogMapping[134] = "BLOCK_TRANSITION_BRDCST"
	LogMapping[136] = "STATE_TRANSITION_REQ"
	LogMapping[137] = "STATE_TRANSITION_RES"
	LogMapping[138] = "GENESIS_RES"
	LogMapping[139] = "BLOCK_TRANSITION_RES"
	LogMapping[141] = "BLOCK_TRANSITION_REQ"
	LogMapping[142] = "SHARD_BLOCK_BRDCST"
	LogMapping[143] = "FIRST_SHARD_BLOCK_RES"
	LogMapping[144] = "FIRST_SHARD_BLOCK_REQ"




}
