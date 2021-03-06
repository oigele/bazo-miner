package miner

const (
	//How many blocks can we verify dynamically (e.g. proper time check) until we are too far behind
	//that this dynamic check is not possible anymore?!
	DELAYED_BLOCKS = 10

	TXFETCH_TIMEOUT    = 2 //Sec
	BLOCKFETCH_TIMEOUT = 40 //Sec
	GENESISFETCH_TIMEOUT 	= 40 //Sec


	//Some prominent programming languages (e.g., Java) have not unsigned integer types
	//Neglecting MSB simplifies compatibility
	MAX_MONEY = 9223372036854775807 //(2^63)-1

	//Default Block params
	BLOCKHASH_SIZE       	= 32      //Byte
	FEE_MINIMUM          	= 1       //Coins
	BLOCK_SIZE           	= 800 	  //Byte
	DIFF_INTERVAL        	= 10      //Blocks
	BLOCK_INTERVAL       	= 15      //Sec
	BLOCK_REWARD         	= 0       //Coins
	STAKING_MINIMUM      	= 100    //Coins
	STAKING_FIX				= 100000
	WAITING_MINIMUM      	= 0       //Blocks
	ACCEPTED_TIME_DIFF   	= 60      //Sec
	SLASHING_WINDOW_SIZE 	= 100     //Blocks
	SLASH_REWARD         	= 2       //Coins
	NUM_INCL_PREV_PROOFS 	= 5       //Number of previous proofs included in the PoS condition
	NO_EMPTYING_LENGTH		= 100	  //Number of blocks after the newest block which are not moved to the empty block bucket
	EPOCH_LENGTH         = 1 //blocks
	VALIDATORS_PER_SHARD = 1 //validators
	EPOCHBLOCKFETCH_TIMEOUT 	= 20 //Sec
	PERCENTAGE_NEEDED_FOR_SLASHING = 0.6667  //use a number between 0 and 1 as percentage, where 0 is 0% and 1 is 100%. 0.6667 stands for 66.67%
	DEFAULT_FINE_SHARD 			=  10 //standard fine if a shard is fined
	DEFAULT_FINE_COMMITTEE      =  25 //standard fine if a committee is fined
)
