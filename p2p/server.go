package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/oigele/bazo-miner/storage"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	//List of ip addresses. A connection to a subset of the list will be established as soon as the network health
	//monitor triggers.
	Ipport string
	peers  peersStruct

	iplistChan      = make(chan string, MIN_MINERS * MIN_MINERS)
	minerBrdcstMsg  = make(chan []byte, 1000)
	minerTxBrdcstMsg= make(chan []byte, 1000)
	clientBrdcstMsg = make(chan []byte)
	register        = make(chan *peer, MIN_MINERS)
	disconnect      = make(chan *peer)
	lastpeerMutex	= &sync.Mutex{}
	lastTriedPeer string
)

//Entry point for p2p package
func Init(ipport string) {
	Ipport = ipport
	InitLogging()

	//Initialize peer map
	peers.minerConns = make(map[*peer]bool)
	peers.clientConns = make(map[*peer]bool)

	//Start all services that are running concurrently
	//----------------------------------------------//
	//add and delete peers
	go peerService()
	//waits for stuff to broadcast. Careful enough with concurrency?
	go minerBroadcastService()
	go minerTxBroadcastService()
	go clientBroadcastService()
	go checkHealthService()
	go timeService()
	go forwardBlockBrdcstToMiner()
	go forwardStateTransitionShardToMiner()
	go forwardCommitteeCheckToMiner()
	go forwardCommitteeCheckRequestToMiner()
	go forwardShardBlockRequestToMiner()
	go forwardTransactionAssignmentRequestToMiner()
	go forwardStateTransitionBrdcstToMiner()
	go forwardTransactionAssignmentBrdcstToMiner()
	go forwardEpochBlockBrdcstToMiner()
	go forwardBlockHeaderBrdcstToMiner()
	go forwardVerifiedTxsToMiner()
	go forwardVerifiedTxsBrdcstToMiner()

	if !IsBootstrap() {
		bootstrap()
	}

	//Listen for all subsequent incoming connections on specified local address/listening port
	go listener(Ipport)
}

func bootstrap() {
	//Connect to bootstrap server. To make it more fault-tolerant, we can increase the number of bootstrap servers in
	//the future. initiateNewMinerConn(...) starts with MINER_PING to perform the initial handshake message
	p, err := initiateNewMinerConnection(storage.Bootstrap_Server)
	if err != nil {
		logger.Printf("Initiating new miner connection failed: %v", err)
	}

	go peerConn(p)
}


func initiateNewMinerConnection(dial string) (*peer, error) {
	var conn net.Conn

	//Check if we already established a dial with that ip or if the ip belongs to us
	if peerExists(dial) {
		return nil, errors.New(fmt.Sprintf("Connection with %v already established.", dial))
	}

	if peerSelfConn(dial) {
		return nil, errors.New(fmt.Sprintf("Cannot self-connect %v.", dial))
	}

	//Open up a tcp dial and instantiate a peer struct, wait for adding it to the peerStruct before we finalize
	//the handshake
	conn, err := net.Dial("tcp", dial)
	p := newPeer(conn, strings.Split(dial, ":")[1], PEERTYPE_MINER)
	if err != nil {
		return nil, err
	}

	//Extracts the port from our localConn variable (which is in the form IP:Port)
	localPort, err := strconv.Atoi(strings.Split(Ipport, ":")[1])
	if err != nil {
		conn.Close()
		return nil, errors.New(fmt.Sprintf("Parsing port failed: %v\n", err))
	}

	packet, err := PrepareHandshake(MINER_PING, localPort)
	if err != nil {
		conn.Close()
		return nil, err
	}

	conn.Write(packet)

	//Wait for the other party to finish the handshake with the corresponding message
	header, _, err := RcvData(p)
	if err != nil || header.TypeID != MINER_PONG {
		conn.Close()
		return nil, errors.New(fmt.Sprintf("Failed to complete miner handshake: %v", err))
	}

	return p, nil
}

func PrepareHandshake(pingType uint8, localPort int) ([]byte, error) {
	//We need to additionally send our local listening port in order to construct a valid first message
	//This will be the only time we need it so we don't save it
	portBuf := make([]byte, PORT_SIZE)
	binary.BigEndian.PutUint16(portBuf[:], uint16(localPort))
	packet := BuildPacket(pingType, portBuf)

	return packet, nil
}

//listener keeps listening to new connections
//the argument is the node's own IPPort
func listener(ipport string) {
	//Listen on all interfaces, this NAT stuff easier
	listener, err := net.Listen("tcp", ":"+strings.Split(ipport, ":")[1])
	if err != nil {
		logger.Printf("%v\n", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			//logger.Printf("%v\n", err)
			continue
		}

		conn.(*net.TCPConn).SetKeepAlive(true)
		conn.(*net.TCPConn).SetKeepAlivePeriod(1 * time.Minute)

		//the listener port is kept empty here. It will be finished after a handshake is performed
		p := newPeer(conn, "", 0)
		//infinite for loop means that this goroutine will run indefinitely
		//this goroutine will read the content of the newly instantiated message
		go handleNewConn(p)
	}
}

func handleNewConn(p *peer) {
	//logger.Printf("New incoming connection: %v\n", p.conn.RemoteAddr().String())

	header, payload, err := RcvData(p)
	if err != nil {
		logger.Printf("Failed to handle incoming connection: %v\n", err)
		return
	}

	processIncomingMsg(p, header, payload)
}

func peerConn(p *peer) {
	if p.peerType == PEERTYPE_MINER {
		logger.Printf("Adding a new miner: %v\n", p.getIPPort())
		neighborBrdcst()
	} else if p.peerType == PEERTYPE_CLIENT {
		//logger.Printf("Adding a new client: %v\n", p.getIPPort())
	}

	//Give the peer a channel
	p.ch = make(chan []byte, 100)

	//Register withe the broadcast service and start the additional writer
	register <- p
	go peerBroadcast(p)

	for {
		header, payload, err := RcvData(p)
		logger.Printf("Received message from %s", p.getIPPort())
		if err != nil {
			if p.peerType == PEERTYPE_MINER {
				logger.Printf("Miner disconnected: %v\n", err)
				disconnect <- p
				time.Sleep(time.Second)
				lastpeerMutex.Lock()
				if getLastTriedPeer() != p.getIPPort() {
					logger.Printf("Try To Reconnect to %v", p.getIPPort())
					//iplistChan gets consumed in checkhelthservice
					iplistChan <- p.getIPPort()
					setLastTriedPeer(p.getIPPort())
				}
				lastpeerMutex.Unlock()
				return
			} else if p.peerType == PEERTYPE_CLIENT {
				//logger.Printf("Client disconnected: %v\n", err)
				disconnect <- p
			}

			return
		}

		go processIncomingMsg(p, header, payload)

	}
}

func setLastTriedPeer(ipPort string) () {
	lastTriedPeer = ipPort
}

func getLastTriedPeer() (string) {
	return lastTriedPeer
}
