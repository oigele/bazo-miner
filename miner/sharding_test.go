package miner

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/crypto"
	"github.com/oigele/bazo-miner/p2p"
	"github.com/oigele/bazo-miner/protocol"
	"github.com/oigele/bazo-miner/storage"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const(
	NodesDirectory 		= "nodes/"
)

var (
	NodeNames			[]string
	TotalNodes			int
)

func TestCommitteeGoRoutines(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)

	var nodeMap1 = make(map[[32]byte]*ecdsa.PrivateKey)
	var nodeMap2 = make(map[[32]byte]*ecdsa.PrivateKey)
	var nodeMap3 = make(map[[32]byte]*ecdsa.PrivateKey)
	var nodeMap4 = make(map[[32]byte]*ecdsa.PrivateKey)


	for i := 1; i <= 60; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		//send to the address of the committee
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 30 hashers to a new map
		if i % 4 == 0{
			nodeMap1[hasherNewNode] = newAccAddress
		} else if i % 4 == 1 {
			nodeMap2[hasherNewNode] = newAccAddress
		} else if i % 4 == 2 {
			nodeMap3[hasherNewNode] = newAccAddress
		} else if i % 4 == 3 {
			nodeMap4[hasherNewNode] = newAccAddress
		}
	}


	numberOfRounds := 20
	j := 150




	start := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(4)



	go func() {
		for i := 1; i <= numberOfRounds; i++ {
			for hasher,_  := range nodeMap1 {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(i*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap1[hasher],
						fromPrivKey,
						nil)

					if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
		wg.Done()
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		for i := 1; i <= numberOfRounds; i++ {
			for hasher,_  := range nodeMap2 {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(i*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap2[hasher],
						fromPrivKey,
						nil)

					if err := SendTx2("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
		wg.Done()
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		for i := 1; i <= numberOfRounds; i++ {
			for hasher,_  := range nodeMap3 {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(i*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap3[hasher],
						fromPrivKey,
						nil)

					if err := SendTx3("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
		wg.Done()
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		for i := 1; i <= numberOfRounds; i++ {
			for hasher,_  := range nodeMap4 {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(i*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap4[hasher],
						fromPrivKey,
						nil)

					if err := SendTx4("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
		wg.Done()
	}()


	wg.Wait()

	t.Log("Waiting for goroutines to finish")

	elapsed := time.Now().Sub(start)


	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())


}

func TestDataTx(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)

	var nodeMap = make(map[[32]byte]*ecdsa.PrivateKey)

	for i := 1; i <= 10; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		//send to the address of the committee
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 30 hashers to a new map
		nodeMap[hasherNewNode] = newAccAddress
	}


	z := 1
	numberOfRounds := 20
	j := 150


	start := time.Now()

	for z = 1; z <= 15; z++ {
		for hasher,_  := range nodeMap {
			for txCount := 1; txCount <= j; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					//can do it like this because no txcount check. the important part is that the txcount is unique
					uint32(z*j-txCount),
					hasher,
					hasherRootNode,
					nodeMap[hasher],
					fromPrivKey,
					[]byte(String(random(1, 10))))

				if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
		}
	}

	for z = 1; z <= numberOfRounds; z++ {
		for hasher,_  := range nodeMap {
			for txCount := 1; txCount <= j; txCount++ {
				tx, _ := protocol.ConstrDataTx(
					byte(0),
					uint64(1),
					//can do it like this because no txcount check. the important part is that the txcount is unique
					uint32(z*j-txCount),
					hasher,
					hasherRootNode,
					nodeMap[hasher],
					fromPrivKey,
					[]byte(String(random(1, 10))))

				if err := SendTx("127.0.0.1:8002", tx, p2p.DATATX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
		}
	}

	/*for z = 1; z <= numberOfRounds; z++ {
		for hasher,_  := range nodeMap {
			for txCount := 1; txCount <= j; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					//can do it like this because no txcount check. the important part is that the txcount is unique
					uint32(z*j-txCount),
					hasher,
					hasherRootNode,
					nodeMap[hasher],
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
		}
	}*/

	elapsed := time.Now().Sub(start)


	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())

}

func TestCommittee(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)

	var nodeMap = make(map[[32]byte]*ecdsa.PrivateKey)

	for i := 1; i <= 30; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		//send to the address of the committee
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		/*if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8003", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}*/

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 30 hashers to a new map
		nodeMap[hasherNewNode] = newAccAddress
	}




	z := 1
	numberOfRounds := 20
	j := 150




	start := time.Now()

	for z = 1; z <= numberOfRounds; z++ {
		for hasher,_  := range nodeMap {
			for txCount := 1; txCount <= j; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					//can do it like this because no txcount check. the important part is that the txcount is unique
					uint32(z*j-txCount),
					hasher,
					hasherRootNode,
					nodeMap[hasher],
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
		}
	}

	elapsed := time.Now().Sub(start)


	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())

}

func TestShardInitiator(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	var nodeMap = make(map[[32]byte]*ecdsa.PrivateKey)

	//create 30 new accounts
	for i := 1; i <= 30; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		if err := SendTx("127.0.0.1:8000", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		/*if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8003", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}*/

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 30 hashers to a new map
		nodeMap[hasherNewNode] = newAccAddress
	}


	i := 1
	/*z := 1
	y := 1
	x := 1*/

	for hasherNewNode, _ := range nodeMap {
	//send funds to new account. Need A LOT
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(1000000),
			uint64(1),
			uint32(i),
			hasherRootNode,
			hasherNewNode,
			fromPrivKey,
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		/*if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8003", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}*/

		i += 1
	}

	for hasherNewNode, _ := range nodeMap {
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(2),
			uint64(1),
			uint32(i),
			hasherNewNode,
			hasherRootNode,
			nodeMap[hasherNewNode],
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		/*if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8003", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}*/

		i += 1

	}


}

func TestShardSemiIterative(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	var nodeMap = make(map[[32]byte]*ecdsa.PrivateKey)

	//create 30 new accounts
	for i := 1; i <= 30; i++ {

		accTx, newAccAddress, _ := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)


		if err := SendTx("127.0.0.1:8000", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8002", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		/*if err := SendTx("127.0.0.1:8003", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}*/

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 30 hashers to a new list
		nodeMap[hasherNewNode] = newAccAddress

		t.Log(hasherNewNode)
	}


	i := 1
	z := 1
	y := 1
	/*x := 1*/

	for hasherNewNode, _ := range nodeMap {
		//send funds to new account
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(100000),
			uint64(1),
			uint32(i),
			hasherRootNode,
			hasherNewNode,
			fromPrivKey,
			fromPrivKey,
			nil)


		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		/*if err := SendTx("127.0.0.1:8003", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}*/

		i += 1

	}




	var wg sync.WaitGroup
	//because we send to x nodes
	wg.Add(3)
	start := time.Now()

	//amount of tx per inner loop
	j := 150

	numberOfRounds := 20

//1st fromprivkey
//2nd multisig priv key (root priv key)

	go func() {
		defer wg.Done()
		for i = 1; i <= numberOfRounds; i++ {
			for hasher,_  := range nodeMap {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(i*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap[hasher],
						fromPrivKey,
						nil)

					if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		defer wg.Done()
		for z = 1; z <= numberOfRounds; z++ {
			for hasher,_  := range nodeMap {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(z*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap[hasher],
						fromPrivKey,
						nil)

					if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		defer wg.Done()
		for y = 1; y <= numberOfRounds; y++ {
			for hasher,_  := range nodeMap {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(y*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap[hasher],
						fromPrivKey,
						nil)

					if err := SendTx3("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
	}()

	/*go func() {
		defer wg.Done()
		for x = 1; x <= numberOfRounds; x++ {
			for hasher,_  := range nodeMap {
				for txCount := 1; txCount <= j; txCount++ {
					tx, _ := protocol.ConstrFundsTx(
						byte(0),
						uint64(10),
						uint64(1),
						//can do it like this because no txcount check. the important part is that the txcount is unique
						uint32(x*j-txCount),
						hasher,
						hasherRootNode,
						nodeMap[hasher],
						fromPrivKey,
						nil)

					if err := SendTx4("127.0.0.1:8003", tx, p2p.FUNDSTX_BRDCST); err != nil {
						t.Log(fmt.Sprintf("Error"))
					}
				}
			}
		}
	}()*/

	wg.Wait()

	t.Log("Waiting for goroutines to finish")

	elapsed := time.Now().Sub(start)


	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())


}


func TestShardIterative(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	var nodeList [][32]byte

	//create 30 new accounts
	for i := 1; i <= 30; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		if err := SendTx("127.0.0.1:8000", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 20 hashers to a new list
		nodeList = append(nodeList, hasherNewNode)

		t.Log(hasherNewNode)
	}


	i := 1

	for _, hasherNewNode := range nodeList {


		//send funds to new account
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(1000000),
			uint64(1),
			uint32(i),
			hasherRootNode,
			hasherNewNode,
			fromPrivKey,
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}

		i += 1
	}



	start := time.Now()

	//amount of tx per inner loop
	j := 150

	for i = 1; i <= 20; i++ {
		for _, hasher := range nodeList {
			for txCount := 1; txCount <= j; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					//can do it like this because no txcount check. the important part is that the txcount is unique
					uint32(i*j - txCount),
					hasher,
					hasherRootNode,
					fromPrivKey,
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
				if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
		}
	}


	t.Log("Waiting for goroutines to finish")

	elapsed := time.Now().Sub(start)

	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())




}

func TestShard(t *testing.T) {

	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	var nodeList [][32]byte

	//create 20 new accounts
	for i := 1; i <= 30; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		if err := SendTx("127.0.0.1:8000", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 20 hashers to a new list
		nodeList = append(nodeList, hasherNewNode)

		t.Log(hasherNewNode)

	}

	//time.Sleep(200 * time.Second)

	i := 1

	for _, hasherNewNode := range nodeList {

		//send funds to new account
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(1000000),
			uint64(1),
			uint32(i),
			hasherRootNode,
			hasherNewNode,
			fromPrivKey,
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}
		if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			t.Log(fmt.Sprintf("Error"))
		}

		i += 1

	}

	var wg sync.WaitGroup

	start := time.Now()

	time.Sleep(10 * time.Millisecond)

	wg.Add(len(nodeList))

	for _, hasher := range nodeList {
		go func([32]byte, *sync.WaitGroup) {
			defer wg.Done()
			//now send tx back to the root account. Note that I never did anything manually with the new account
			for txCount := 0; txCount < 1000; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(10),
					uint32(txCount),
					hasher,
					hasherRootNode,
					fromPrivKey,
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
				/*if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}*/
			}
		}(hasher, &wg)
		//concurrency safety
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	elapsed := time.Now().Sub(start)

	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())

}

func TestAccountCreation(t *testing.T) {

	rootNode := fmt.Sprintf("WalletA.txt")
	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	rootNodePrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)
	var nodeList [][32]byte

	//create 10 new accounts
	for i := 1; i <= 20; i++ {

		accTx, newAccAddress, err := protocol.ConstrAccTx(
			byte(0),
			uint64(1),
			[64]byte{},
			rootNodePrivKey,
			nil,
			nil)

		if err != nil {
			t.Log("got an issue")
		}

		if err := SendTx("127.0.0.1:8000", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8001", accTx, p2p.ACCTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		newNodeAddress := crypto.GetAddressFromPubKey(&newAccAddress.PublicKey)
		hasherNewNode := protocol.SerializeHashContent(newNodeAddress)

		//append all 20 hashers to a new list
		nodeList = append(nodeList, hasherNewNode)

		t.Log(hasherNewNode)
		time.Sleep(10 * time.Second)

	}

	//time.Sleep(200 * time.Second)

	i := 1

	for _, hasherNewNode := range nodeList {


		//send funds to new account
		tx, _ := protocol.ConstrFundsTx(
			byte(0),
			uint64(1000000),
			uint64(1),
			uint32(i),
			hasherRootNode,
			hasherNewNode,
			fromPrivKey,
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}
		if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
			fmt.Sprintf("Error")
		}

		i += 1

		time.Sleep(2 * time.Second)

	}


	time.Sleep(60 * time.Second)

	var wg sync.WaitGroup



	wg.Add(len(nodeList))

	start := time.Now()

	for _, hasher := range nodeList {
		go func([32]byte, *sync.WaitGroup) {
			defer wg.Done()
			t.Log(hasher[0:8])
			//now send tx back to the root account. Note that I never did anything manually with the new account
			for txCount := 0; txCount < 10000; txCount++ {
				tx, _ := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					uint32(txCount),
					hasher,
					hasherRootNode,
					fromPrivKey,
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
				if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(fmt.Sprintf("Error"))
				}
			}
			t.Log("One is finished")
		}(hasher, &wg)
		time.Sleep(2 * time.Second)
	}


	t.Log("Waiting for goroutines to finish")

	elapsed1 := time.Now().Sub(start)

	t.Log(elapsed1.Seconds())

	wg.Wait()

	elapsed := time.Now().Sub(start)

	t.Log(elapsed.Seconds())
	t.Log(elapsed.Nanoseconds())

}


func TestOneSender(t *testing.T) {
	rootNode := fmt.Sprintf("WalletA.txt")
	firstNode := fmt.Sprintf("WalletB.txt")

	rootNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(rootNode)
	firstNodePubKey, _ := crypto.ExtractECDSAPublicKeyFromFile(firstNode)

	rootNodeAddress := crypto.GetAddressFromPubKey(rootNodePubKey)
	firstNodeAddress := crypto.GetAddressFromPubKey(firstNodePubKey)


	hasherRootNode := protocol.SerializeHashContent(rootNodeAddress)
	hasherFirstNode := protocol.SerializeHashContent(firstNodeAddress)

	t.Log(hasherRootNode)
	t.Log(hasherFirstNode)

	fromPrivKey, _ := crypto.ExtractECDSAKeyFromFile(rootNode)

	time.Sleep(5 * time.Second)

	// send a lot of funds to the second node
	tx,_ := protocol.ConstrFundsTx(
		byte(0),
		uint64(1000000),
		uint64(1),
		uint32(1),
		hasherRootNode,
		hasherFirstNode,
		fromPrivKey,
		fromPrivKey,
		nil)

	if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
		fmt.Sprintf("Error")
	}
	if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
		fmt.Sprintf("Error")
	}

	time.Sleep(60 * time.Second)

	tx,_ = protocol.ConstrFundsTx(
		byte(0),
		uint64(500000),
		uint64(1),
		uint32(0),
		hasherFirstNode,
		hasherRootNode,
		fromPrivKey,
		fromPrivKey,
		nil)


	if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
		fmt.Sprintf("Error")
	}
	if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
		fmt.Sprintf("Error")
	}

	time.Sleep(100 * time.Second)

	go func() {
		txCount := 1
		for {
			tx, _ := protocol.ConstrFundsTx(
				byte(0),
				uint64(10),
				uint64(1),
				uint32(txCount),
				hasherFirstNode,
				hasherRootNode,
				fromPrivKey,
				fromPrivKey,
				nil)
			if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
				fmt.Sprintf("Error")
				continue
			}
			if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
				fmt.Sprintf("Error")
				continue
			}
			txCount += 1
		}
	}()
	go func() {
		txCount := 2
		for {
			tx, _ := protocol.ConstrFundsTx(
				byte(0),
				uint64(10),
				uint64(1),
				uint32(txCount),
				hasherRootNode,
				hasherFirstNode,
				fromPrivKey,
				fromPrivKey,
				nil)
			if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
				fmt.Sprintf("Error")
				continue
			}
			if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
				fmt.Sprintf("Error")
				continue
			}
			txCount += 1
		}
	}()


	time.Sleep(45 * time.Second)
	t.Log("Done!!!")


}

func TestShardingWith20Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestShardingWith20Nodes...")
	}
	/**
	Set Total Number of desired nodes. They will be generated automatically. And for each node, a separate go routine is being created.
	This enables parallel issuance of transactions
	 */
	TotalNodes = 20

	//Generate wallet directories for all nodes, i.e., validators and non-validators
	for i := 1; i <= TotalNodes; i++ {
		strNode := fmt.Sprintf("Node_%d",i)
		if(!stringAlreadyInSlice(NodeNames,strNode)){
			NodeNames = append(NodeNames,strNode)
		}
		if _, err := os.Stat(NodesDirectory+strNode); os.IsNotExist(err) {
			err = os.MkdirAll(NodesDirectory+strNode, 0755)
			if err != nil {
				t.Errorf("Error while creating node directory %v\n",err)
			}
		}
		storage.Init(NodesDirectory+strNode+"storage.db", TestIpPort)
		_, err := crypto.ExtractECDSAPublicKeyFromFile(NodesDirectory+strNode+"wallet.txt")
		if err != nil {
			return
		}
		_, err = crypto.ExtractRSAKeyFromFile(NodesDirectory+strNode+"commitment.txt")
		if err != nil {
			return
		}
	}
	//
	time.Sleep(30*time.Second)
	//var wg sync.WaitGroup

	transferFundsToWallets()

	time.Sleep(20*time.Second)

	//Create a goroutine for each wallet and send TX from corresponding wallet to root account
	for i := 1; i <= TotalNodes; i++ {
		strNode := fmt.Sprintf("Node_%d",i)
		//wg.Add(1)
		go func() {
			txCount := 0
			//for i := 1; i <= 500; i++ {
			for {
				fromPrivKey, err := crypto.ExtractECDSAKeyFromFile(NodesDirectory+strNode+"wallet.txt")
				if err != nil {
					return
				}

				toPubKey, err := crypto.ExtractECDSAPublicKeyFromFile("WalletA.txt")
				if err != nil {
					return
				}

				fromAddress := crypto.GetAddressFromPubKey(&fromPrivKey.PublicKey)
				//t.Logf("fromAddress: (%x)\n",fromAddress[0:8])
				toAddress := crypto.GetAddressFromPubKey(toPubKey)
				//t.Logf("toAddress: (%x)\n",toAddress[0:8])

				tx, err := protocol.ConstrFundsTx(
					byte(0),
					uint64(10),
					uint64(1),
					uint32(txCount),
					protocol.SerializeHashContent(fromAddress),
					protocol.SerializeHashContent(toAddress),
					fromPrivKey,
					fromPrivKey,
					nil)

				if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(err)
					continue
				}
				if err := SendTx("127.0.0.1:8001", tx, p2p.FUNDSTX_BRDCST); err != nil {
					t.Log(err)
					continue
				}
				//if err := SendTx("127.0.0.1:8002", tx, p2p.FUNDSTX_BRDCST); err != nil {
				//	continue
				//}
				txCount += 1
			}
			//wg.Done()
		}()
	}

	time.Sleep(45*time.Second)

	//wg.Wait()
	t.Log("Done...")
}

func TestSendingFundsTo20Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestSendingFundsTo20Nodes...")
	}
	TotalNodes = 20
	transferFundsToWallets()
}

func transferFundsToWallets() {
	//Transfer 1 Mio. funds to all wallets from root account
	txCountRootAccBeginning := 0
	for i := 1; i <= TotalNodes; i++ {
		strNode := fmt.Sprintf("Node_%d",i)
		fromPrivKey, err := crypto.ExtractECDSAKeyFromFile("walletMinerA.key")
		if err != nil {
			return
		}

		toPubKey, err := crypto.ExtractECDSAPublicKeyFromFile(NodesDirectory+strNode+"/wallet.key")
		if err != nil {
			return
		}

		fromAddress := crypto.GetAddressFromPubKey(&fromPrivKey.PublicKey)
		toAddress := crypto.GetAddressFromPubKey(toPubKey)

		tx, err := protocol.ConstrFundsTx(
			byte(0),
			uint64(10000),
			uint64(1),
			uint32(txCountRootAccBeginning),
			protocol.SerializeHashContent(fromAddress),
			protocol.SerializeHashContent(toAddress),
			fromPrivKey,
			fromPrivKey,
			nil)

		if err := SendTx("127.0.0.1:8000", tx, p2p.FUNDSTX_BRDCST); err != nil {
			return
		}
		txCountRootAccBeginning += 1
	}
}

func SendTx(dial string, tx protocol.Transaction, typeID uint8) (err error) {
	if conn := p2p.Connect(dial); conn != nil {
		packet := p2p.BuildPacket(typeID, tx.Encode())
		conn.Write(packet)

		header, payload, err := p2p.RcvData_(conn)
		if err != nil || header.TypeID == p2p.NOT_FOUND {
			err = errors.New(string(payload[:]))
		}
		conn.Close()

		return err
	}

	txHash := tx.Hash()
	return errors.New(fmt.Sprintf("Sending tx %x failed.", txHash[:8]))
}


func SendTx2(dial string, tx protocol.Transaction, typeID uint8) (err error) {
	if conn := p2p.Connect(dial); conn != nil {
		packet := p2p.BuildPacket(typeID, tx.Encode())
		conn.Write(packet)

		header, payload, err := p2p.RcvData_(conn)
		if err != nil || header.TypeID == p2p.NOT_FOUND {
			err = errors.New(string(payload[:]))
		}
		conn.Close()

		return err
	}

	txHash := tx.Hash()
	return errors.New(fmt.Sprintf("Sending tx %x failed.", txHash[:8]))
}

func SendTx3(dial string, tx protocol.Transaction, typeID uint8) (err error) {
	if conn := p2p.Connect(dial); conn != nil {
		packet := p2p.BuildPacket(typeID, tx.Encode())
		conn.Write(packet)

		header, payload, err := p2p.RcvData_(conn)
		if err != nil || header.TypeID == p2p.NOT_FOUND {
			err = errors.New(string(payload[:]))
		}
		conn.Close()

		return err
	}

	txHash := tx.Hash()
	return errors.New(fmt.Sprintf("Sending tx %x failed.", txHash[:8]))
}

func SendTx4(dial string, tx protocol.Transaction, typeID uint8) (err error) {
	if conn := p2p.Connect(dial); conn != nil {
		packet := p2p.BuildPacket(typeID, tx.Encode())
		conn.Write(packet)

		header, payload, err := p2p.RcvData_(conn)
		if err != nil || header.TypeID == p2p.NOT_FOUND {
			err = errors.New(string(payload[:]))
		}
		conn.Close()

		return err
	}

	txHash := tx.Hash()
	return errors.New(fmt.Sprintf("Sending tx %x failed.", txHash[:8]))
}


func stringAlreadyInSlice(inputSlice []string, str string) bool {
	for _, entry := range inputSlice {
		if entry == str {
			return true
		}
	}
	return false
}

//code for string randomization taken from https://www.calhoun.io/creating-random-strings-in-go/
const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}
//end code taken from https://www.calhoun.io/creating-random-strings-in-go/

//function taken from https://golangcode.com/generate-random-numbers/
func random(min int, max int) int {
	return rand.Intn(max-min) + min
}
//end function taken from https://golangcode.com/generate-random-numbers/