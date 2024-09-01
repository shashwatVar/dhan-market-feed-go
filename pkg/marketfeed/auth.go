package marketfeed

import (
	"context"

	"github.com/gorilla/websocket"
	"github.com/shashwatVar/dhan-market-feed-go/internal/utils"
)

func (mf *MarketFeed) Authorize(ctx context.Context) error {
	authPacket := mf.createAuthPacket()
	err := mf.ws.WriteMessage(websocket.BinaryMessage, authPacket)
	if err != nil {
		return err
	}

	return nil
}

func (mf *MarketFeed) createAuthPacket() []byte {
	messageLength := uint16(585) // 83 (header) + 500 (API token) + 2 (Auth Type)
	packet := make([]byte, messageLength)

	header := mf.createHeaderPacket(11, messageLength, mf.clientID) // 11 is the Feed Request Code for new connection
	copy(packet[:83], header)

	// API Access Token (500 bytes)
	copy(packet[83:583], utils.PadOrTruncate([]byte(mf.accessToken), 500))

	// Authentication Type (2 bytes)
	packet[583] = '2'
	packet[584] = 'P'

	return packet
}