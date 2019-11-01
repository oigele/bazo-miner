package p2p

import "fmt"

const HEADER_LEN = 5

//Mapping constants, used to parse incoming messages
const (
	FUNDSTX_BRDCST     		= 1
	ACCTX_BRDCST       		= 2
	CONFIGTX_BRDCST    		= 3
	STAKETX_BRDCST     		= 4
	VERIFIEDTX_BRDCST  		= 5
	BLOCK_BRDCST       		= 6
	BLOCK_HEADER_BRDCST		= 7
	TX_BRDCST_ACK      		= 8
	AGGTX_BRDCST      		= 9

	GENESIS_REQ			    = 19
	FUNDSTX_REQ            	= 20
	ACCTX_REQ              	= 21
	CONFIGTX_REQ           	= 22
	STAKETX_REQ            	= 23
	BLOCK_REQ              	= 24
	BLOCK_HEADER_REQ       	= 25
	ACC_REQ                	= 26
	ROOTACC_REQ            	= 27
	INTERMEDIATE_NODES_REQ 	= 28
	AGGTX_REQ				= 29
	UNKNOWNTX_REQ			= 30
	SPECIALTX_REQ			= 31
	NOT_FOUND_TX_REQ		= 32


	FUNDSTX_RES            	= 40
	ACCTX_RES              	= 41
	CONFIGTX_RES           	= 42
	STAKETX_RES            	= 43
	BLOCK_RES              	= 44
	BlOCK_HEADER_RES       	= 45
	ACC_RES                	= 46
	ROOTACC_RES            	= 47
	INTERMEDIATE_NODES_RES 	= 48
	AGGTX_RES				= 49

	NEIGHBOR_REQ = 130
	NEIGHBOR_RES = 140

	TIME_BRDCST = 150

	MINER_PING  = 100
	MINER_PONG  = 101
	CLIENT_PING = 102
	CLIENT_PONG = 103

	//Used to signal error
	NOT_FOUND = 110


	STATE_REQ = 120
	STATE_RES = 121

	FIRST_EPOCH_BLOCK_REQ = 122
	FIRST_EPOCH_BLOCK_RES = 123
	EPOCH_BLOCK_REQ = 124
	EPOCH_BLOCK_RES = 125
	VALIDATOR_SHARD_BRDCST = 126
	VALIDATOR_SHARD_REQ = 127
	VALIDATOR_SHARD_RES = 128
	EPOCH_BLOCK_BRDCST        = 129
	LAST_EPOCH_BLOCK_RES = 131
	LAST_EPOCH_BLOCK_REQ = 132
	STATE_TRANSITION_BRDCST = 133
	STATE_TRANSITION_REQ = 136
	STATE_TRANSITION_RES = 137
	GENESIS_RES = 138
)

type Header struct {
	Len    uint32
	TypeID uint8
}

func (header Header) String() string {
	return fmt.Sprintf(
		"Length: %v\n"+
			"TypeID: %v\n",
		header.Len,
		header.TypeID,
	)
}
