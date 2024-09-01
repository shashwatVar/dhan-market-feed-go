package marketfeed

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/gorilla/websocket"
	"github.com/shashwatVar/dhan-market-feed-go/internal/utils"
)


const (
	maxInstrumentsPerBatch = 100
)

// this can also be used to unsubscribe -> just need to change the feedCode 
func (mf *MarketFeed) SubscribeInstruments(ctx context.Context, instruments []Instrument) error {
	// Group instruments by feedType
	instrumentsByFeedType := make(map[uint16][]Instrument)
	for _, instrument := range instruments {
		instrumentsByFeedType[instrument.FeedType] = append(instrumentsByFeedType[instrument.FeedType], instrument)
	}

	// Subscribe to each feedType batch
	for feedType, instrumentBatch := range instrumentsByFeedType {
		for i := 0; i < len(instrumentBatch); i += maxInstrumentsPerBatch {
			end := i + maxInstrumentsPerBatch
			if end > len(instrumentBatch) {
				end = len(instrumentBatch)
			}
			batch := instrumentBatch[i:end]
			
			packet := mf.createSubscribePacket(feedType, batch)

			err := mf.ws.WriteMessage(websocket.BinaryMessage, packet)
			if err != nil {
				return fmt.Errorf("failed to send subscription packet for feedType %d: %w", feedType, err)
			}
		}
	}
	return nil
}


func (mf *MarketFeed) createSubscribePacket(feedType uint16, instruments []Instrument) []byte {
	numInstruments := len(instruments)
	messageLength := uint16(83 + 4 + numInstruments*21)
	
	packet := make([]byte, 83 + 4 + 100*21)
	
	// Create and copy the header
	header := mf.createHeaderPacket(feedType, messageLength, mf.clientID)
	copy(packet[:83], header)

	// Number of Instruments (4 bytes)
	binary.LittleEndian.PutUint32(packet[83:87], uint32(numInstruments))

	// Instrument Subscription Packets
	for i, instrument := range instruments {
		subscriptionPacket := mf.createInstrumentSubscriptionPacket(instrument.ExchangeSegment, instrument.SecurityID)
		copy(packet[87+(i*21):108+(i*21)], subscriptionPacket)
	}

	return packet
}

func (mf *MarketFeed) createInstrumentSubscriptionPacket(exchangeSegment uint16, securityId string) []byte {
	messageLength := uint16(21) // 1 (Exchange Segment) + 20 (Security ID)
	packet := make([]byte, messageLength)

	// Exchange Segment (1 byte)
	packet[0] = byte(exchangeSegment)

	// Security ID (20 bytes)
	copy(packet[1:21], utils.PadOrTruncate([]byte(securityId), 20))

	return packet
}
