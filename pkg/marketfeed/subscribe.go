package marketfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)


const (
	maxInstrumentsPerBatch = 100
)

type SubscriptionRequest struct {
	RequestCode      int           `json:"RequestCode"`
	InstrumentCount  int           `json:"InstrumentCount"`
	InstrumentList   []SubInstrument `json:"InstrumentList"`
}

type SubInstrument struct {
	ExchangeSegment string `json:"ExchangeSegment"`
	SecurityID      string `json:"SecurityId"`
}

func (mf *MarketFeed) SubscribeInstruments(ctx context.Context, instruments []Instrument) error {
	// Group instruments into batches of 100
	for i := 0; i < len(instruments); i += maxInstrumentsPerBatch {
		end := i + maxInstrumentsPerBatch
		if end > len(instruments) {
			end = len(instruments)
		}
		batch := instruments[i:end]

		instrumentList := make([]SubInstrument, len(batch))
		for j, inst := range batch {
			instrumentList[j] = SubInstrument{
				ExchangeSegment: getExchangeSegmentString(inst.ExchangeSegment),
				SecurityID:      inst.SecurityID,
			}
		}

		request := SubscriptionRequest{
			RequestCode:     15, // Use Ticker feed type for now
			InstrumentCount: len(batch),
			InstrumentList:  instrumentList,
		}

		jsonData, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("failed to marshal subscription request: %w", err)
		}

		// Add debug logging
		log.Printf("Sending subscription request: %s", string(jsonData))

		err = mf.ws.WriteMessage(websocket.TextMessage, jsonData)
		if err != nil {
			return fmt.Errorf("failed to send subscription request: %w", err)
		}
	}
	return nil
}

func getExchangeSegmentString(segment uint16) string {
	switch segment {
	case 0:
		return "IDX_I"
	case 1:
		return "NSE_EQ"
	case 2:
		return "NSE_FNO"
	case 3:
		return "NSE_CURRENCY"
	case 4:
		return "BSE_EQ"
	case 5:
		return "MCX_COMM"
	case 7:
		return "BSE_CURRENCY"
	case 8:
		return "BSE_FNO"
	default:
		log.Printf("Warning: Unknown exchange segment: %d", segment)
		return fmt.Sprintf("UNKNOWN_%d", segment)
	}
}
