package p2p

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/bazo-blockchain/bazo-miner/protocol"
	"github.com/bazo-blockchain/bazo-miner/storage"
	"net"
	"strings"
	"time"
)

func Connect(connectionString string) *net.TCPConn {
	tcpAddr, err := net.ResolveTCPAddr("tcp", connectionString)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)

	if err != nil {
		logger.Printf("Connection to %v failed.\n", connectionString)
		return nil
	}

	conn.SetLinger(0)
	conn.SetDeadline(time.Now().Add(20 * time.Second))

	return conn
}

var (counter = 0)
func RcvData(p *peer) (header *Header, payload []byte, err error) {
	p.mu.Lock()
	counter++
	logger.Printf(" - - RcvData: - START (%v) - RcvData For Peer %v on conn: %x with local address: %v", counter, p.getIPPort(), p.conn, p.conn.LocalAddr())
	defer logger.Printf(" - - RcvData: -  END (%v)  - RcvData For Peer %v on conn: %x with local address: %v", counter, p.getIPPort(), p.conn, p.conn.LocalAddr())
	defer p.mu.Unlock()

	reader := bufio.NewReader(p.conn)
	header, err = ReadHeader(reader)
	if err != nil {
		p.conn.Close()
		if p.peerType == PEERTYPE_MINER {
			logger.Printf(" RcvData %v -- (1) --> %v ", p.getIPPort(), err)
		}
		return nil, nil, errors.New(fmt.Sprintf("Connection to %v aborted: %v", p.getIPPort(), err))
	}

	if int(header.Len) > 800000 { //FABIO
		logger.Printf("Header.Len = %v --> Abort here to prevent an Out Of Memory Error", header.Len)
		p.conn.Close()
		if p.peerType == PEERTYPE_MINER {
			logger.Printf(" RcvData %v -- (2)", p.getIPPort())
		}
		return nil, nil, errors.New(fmt.Sprintf("Abort Receiving Data from %v to prevent Out Of Memory Error", p.getIPPort()))
	}

	payload = make([]byte, header.Len)

	for cnt := 0; cnt < int(header.Len); cnt++ {
		payload[cnt], err = reader.ReadByte()
		if err != nil {
			p.conn.Close()
			if p.peerType == PEERTYPE_MINER {
				logger.Printf(" RcvData %v -- (3) --> %v ", p.getIPPort(), err)
			}
			return nil, nil, errors.New(fmt.Sprintf("Connection to %v aborted: %v", p.getIPPort(), err))
		}
	}


	//logger.Printf("Receive message:\nSender: %v\nType: %v\nPayload length: %v\n", p.getIPPort(), LogMapping[header.TypeID], len(payload))

	return header, payload, nil
}

func RcvData_(c net.Conn) (header *Header, payload []byte, err error) {
	reader := bufio.NewReader(c)
	header, err = ReadHeader(reader)
	if err != nil {
		c.Close()
		return nil, nil, errors.New(fmt.Sprintf("Connection to aborted: (%v)\n", err))
	}
	payload = make([]byte, header.Len)

	for cnt := 0; cnt < int(header.Len); cnt++ {
		payload[cnt], err = reader.ReadByte()
		if err != nil {
			c.Close()
			return nil, nil, errors.New(fmt.Sprintf("Connection to aborted: %v\n", err))
		}
	}

	return header, payload, nil
}

func sendData(p *peer, payload []byte) {
	//logger.Printf("Send message:\nReceiver: %v\nType: %v\nPayload length: %v\n", p.getIPPort(), LogMapping[payload[4]], len(payload)-HEADER_LEN)

	if LogMapping[payload[4]] == "" {
		logger.Printf("Strange Header.TypeID (%v) to send to %v", payload[4], p.getIPPort())
	}
	//logger.Printf("IP: %v, SourceIP: %v", p.getIPPort(), Ipport)
	//logger.Printf("Payload: %x", payload)
	p.conn.Write(payload)

}

//Tested in server_test.go
func peerExists(newIpport string) bool {
	peerList := peers.getAllPeers(PEERTYPE_MINER)

	for _, p := range peerList {
		ipport := p.getIPPort()
		if ipport == newIpport {
			return true
		}
	}

	return false
}

//Tested in server_test.go
func peerSelfConn(newIpport string) bool {
	return newIpport == Ipport
}

func BuildPacket(typeID uint8, payload []byte) (packet []byte) {
	var payloadLen [4]byte

	if int(len(payload)) > 800000 {
		logger.Printf("Payload = %v --> Probably Abort here to prevent an Out Of Memory Error", len(payload))
	}

	packet = make([]byte, HEADER_LEN+len(payload))
	binary.BigEndian.PutUint32(payloadLen[:], uint32(len(payload)))
	copy(packet[0:4], payloadLen[:])
	if LogMapping[typeID] == "" {
		logger.Printf("Build Packet with Strange TypeID: %v", typeID)
	}
	packet[4] = byte(typeID)
	copy(packet[5:], payload)

	return packet
}

func ReadHeader(reader *bufio.Reader) (*Header, error) {
	//The first four bytes of any incoming messages is the length of the payload.
	//Error catching after every read is necessary to avoid panicking.
	var headerArr [HEADER_LEN]byte

	//Reading byte by byte is surprisingly fast and works a lot better for concurrent connections.
	for i := range headerArr {
		extr, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		headerArr[i] = extr
	}

	header := extractHeader(headerArr[:])

	//Check if the type is registered in the protocol.
	if LogMapping[header.TypeID] == "" {
		logger.Printf("Header: TypeID not found. --> typeID: %v", header.TypeID)
		logger.Printf("HEADERARR: %x", headerArr)

		return nil, errors.New("Header: TypeID not found.")
	}

	//Check if the payload length does not exceed the MAX_BLOCK_SIZE defined in configtx.go
	if header.Len > protocol.MAX_BLOCK_SIZE {
		return nil, errors.New("Header: Payload exceeds MAX_BLOCK_SIZE")
	}

	return header, nil
}

//Decoupled functionality for testing reasons.
func extractHeader(headerData []byte) *Header {
	header := new(Header)
	lenBuf := [4]byte{headerData[0], headerData[1], headerData[2], headerData[3]}
	packetLen := binary.BigEndian.Uint32(lenBuf[:])

	header.Len = packetLen
	header.TypeID = uint8(headerData[4])

	return header
}

func IsBootstrap() bool {
	//Set thisPort global, this will be the listening port for incoming connection
	bootstrapPort := strings.Split(storage.Bootstrap_Server, ":")[1]
	thisPort := strings.Split(Ipport, ":")[1]
	if thisPort == bootstrapPort {
		//Only the port is checked if it is bootstrapping... Not the whole IP-Address
		//All Clients need to run on another port than teh bootstrap server....
		return true
	}
	return false
}

