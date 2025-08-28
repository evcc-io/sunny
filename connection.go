// Copyright 2019 Benjamin Böhmke <benjamin@boehmke.net>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sunny

import (
	"fmt"
	"net"
	"slices"
	"sync"

	"gitlab.com/wabuMike/sunny/proto"
)

const listenAddress = "239.12.255.254:9522"

var connectionMutex sync.Mutex
var connections = make(map[string]*Connection)

// Connection for communication with devices
type Connection struct {
	// multicast address
	address *net.UDPAddr
	// multicast socket
	socket *net.UDPConn

	// buffer for received packet
	receiverMutex    sync.RWMutex
	receiverChannels map[string][]chan *proto.Packet

	// interface for device discovery
	discoverMutex    sync.RWMutex
	discoverChannels []chan string
}

// NewConnection creates a new Connection object and starts listening
func NewConnection(inf string) (*Connection, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()

	// connection already known
	if c, ok := connections[inf]; ok {
		return c, nil
	}

	conn := Connection{
		receiverChannels: make(map[string][]chan *proto.Packet),
	}

	var err error
	conn.address, err = net.ResolveUDPAddr("udp", listenAddress)
	if err != nil {
		return nil, err
	}

	// listen interface is optional
	var listenInterface *net.Interface
	if inf != "" {
		listenInterface, err = net.InterfaceByName(inf)
		if err != nil {
			return nil, err
		}
	}

	conn.socket, err = net.ListenMulticastUDP("udp", listenInterface, conn.address)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	err = conn.socket.SetReadBuffer(2048)
	if err != nil {
		return nil, err
	}

	go conn.listenLoop()

	connections[inf] = &conn
	return &conn, nil
}

// listenLoop for received packets
func (c *Connection) listenLoop() {
	b := make([]byte, 2048)

	for c.socket != nil {
		n, src, err := c.socket.ReadFromUDP(b)
		if err != nil {
			// failed to read from udp -> retry
			if DetailedPacketLogging.Load() {
				Log.Printf("DBG: UDP read failed: %v", err)
			}
			continue
		}

		srcIP := src.IP.String()
		var pack proto.Packet
		err = pack.Read(b[:n])
		if err != nil {
			// invalid packet received -> retry
			Log.Printf("recv %s invalid: %v", srcIP, err)
			continue
		}
		Log.Printf("recv %s: [%s]", srcIP, pack)

		c.handleDiscovered(srcIP)
		c.handlePackets(srcIP, &pack)
	}
}

// handlePackets and forward to receivers
func (c *Connection) handlePackets(srcIp string, packet *proto.Packet) {
	c.receiverMutex.RLock()
	defer c.receiverMutex.RUnlock()

	for _, ch := range c.receiverChannels[srcIp] {
		select {
		case ch <- packet:
		default:
			// channel for received packets busy -> drop packet
			if DetailedPacketLogging.Load() {
				Log.Printf("DBG: receiver channel busy -> drop packet from %s: [%s]", srcIp, packet)
			}
		}
	}
}

// registerReceiver channel for a specific IP
func (c *Connection) registerReceiver(srcIp string, ch chan *proto.Packet) {
	c.receiverMutex.Lock()
	defer c.receiverMutex.Unlock()

	c.receiverChannels[srcIp] = append(c.receiverChannels[srcIp], ch)
}

// unregisterReceiver channel for a specific IP
func (c *Connection) unregisterReceiver(srcIp string, ch chan *proto.Packet) {
	c.receiverMutex.Lock()
	defer c.receiverMutex.Unlock()

	receivers, ok := c.receiverChannels[srcIp]
	if !ok {
		return // IP not in list -> no channel to unregister
	}

	c.receiverChannels[srcIp] = slices.DeleteFunc(receivers, func(receiver chan *proto.Packet) bool {
		return receiver == ch
	})
}

// handleDiscovered devices and forward IP to registered channels
func (c *Connection) handleDiscovered(srcIp string) {
	c.discoverMutex.RLock()
	defer c.discoverMutex.RUnlock()

	for _, ch := range c.discoverChannels {
		select {
		case ch <- srcIp:
		default:
			// channel for received packets busy -> drop packet
			if DetailedPacketLogging.Load() {
				Log.Printf("DBG: discover channel busy -> skip notify for %s", srcIp)
			}
		}
	}
}

// registerDiscoverer channel to receive source IP of received device packages
func (c *Connection) registerDiscoverer(ch chan string) {
	c.discoverMutex.Lock()
	defer c.discoverMutex.Unlock()

	c.discoverChannels = append(c.discoverChannels, ch)
}

// unregisterDiscoverer channel
func (c *Connection) unregisterDiscoverer(ch chan string) {
	c.discoverMutex.Lock()
	defer c.discoverMutex.Unlock()

	c.discoverChannels = slices.DeleteFunc(c.discoverChannels, func(entry chan string) bool {
		return entry == ch
	})
}

// sendPacket to the given address
func (c *Connection) sendPacket(address *net.UDPAddr, packet *proto.Packet) error {
	Log.Printf("send %s: [%s]", address.IP.String(), packet)
	_, err := c.socket.WriteToUDP(packet.Bytes(), address)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}
