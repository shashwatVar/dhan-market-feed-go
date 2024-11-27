package marketfeed

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shashwatVar/dhan-market-feed-go/internal/constants"
	"github.com/shashwatVar/dhan-market-feed-go/internal/utils"
)

type MarketFeed struct {
    clientID         string
    accessToken      string
    instruments      []Instrument
    ws               *websocket.Conn
    onConnect        func() error
    onMessage        func(interface{}) error
    onClose          func(error) error
}

type MarketFeedInterface interface {
	Connect(ctx context.Context) error
	Authorize(ctx context.Context) error
	SubscribeInstruments(ctx context.Context, instruments []Instrument) error
	ProcessData(ctx context.Context)
	Disconnect() error
	DisconnectGracefully() error
}

type Instrument struct {
	ExchangeSegment uint16
	SecurityID      string
	FeedType        uint16
}

func NewMarketFeed(clientID, accessToken string, instruments []Instrument,
    onConnect func() error, onMessage func(interface{}) error, onClose func(error) error) MarketFeedInterface {
    return &MarketFeed{
        clientID:         clientID,
        accessToken:      accessToken,
        instruments:      instruments,
        onConnect:        onConnect,
        onMessage:        onMessage,
        onClose:          onClose,
    }
}

func (mf *MarketFeed) Connect(ctx context.Context) error {
	// Construct URL with query parameters
	wsURL := fmt.Sprintf("%s?version=%s&token=%s&clientId=%s&authType=%s",
		constants.WSS_URL_BASE,
		constants.VERSION,
		mf.accessToken,
		mf.clientID,
		constants.AUTH_TYPE,
	)

	// Connect to WebSocket server
	c, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket server: %w", err)
	}
	mf.ws = c

	mf.ws.SetPingHandler(func(appData string) error {
		err := mf.ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
		if err != nil {
			log.Printf("Failed to send pong: %v", err)
		}
		return nil
	})

	// No need for separate authorization in v2 since it's handled via URL params
	
	err = mf.SubscribeInstruments(ctx, mf.instruments)
	if err != nil {
		return fmt.Errorf("failed to subscribe to instruments: %w", err)
	}

	go mf.ProcessData(ctx)

	if mf.onConnect != nil {
		if err := mf.onConnect(); err != nil {
			return fmt.Errorf("onConnect callback failed: %w", err)
		}
	}

	return nil
}

func (mf *MarketFeed) createHeaderPacket(feedRequestCode uint16, messageLength uint16, clientID string) []byte {
	header := make([]byte, 83)
   
    // Feed Request Code (1 byte)
    header[0] = byte(feedRequestCode)
    
    // Message Length (2 bytes)
    binary.LittleEndian.PutUint16(header[1:3], messageLength)
    
    // Client ID (30 bytes)
    copy(header[3:33], utils.PadOrTruncate([]byte(clientID), 30))
   
    // Dhan Auth (50 bytes) - to be passed as zero
    // Already initialized to zero, so no need to explicitly set
    
    return header
}

func (mf *MarketFeed) Disconnect() error {
	if mf.ws == nil {
		return nil // Already disconnected or never connected
	}

	// Close the WebSocket connection
	err := mf.ws.Close()
	if err != nil {
		return fmt.Errorf("error closing WebSocket connection: %w", err)
	}

	// Call the onClose callback if it exists
	if mf.onClose != nil {
		if err := mf.onClose(nil); err != nil {
			return fmt.Errorf("onClose callback failed: %w", err)
		}
	}

	mf.ws = nil // Reset the WebSocket connection
	return nil
}
func (mf *MarketFeed) DisconnectGracefully() error {
    if mf.ws == nil {
        return nil
    }

    // Send disconnect request
    request := struct {
        RequestCode int `json:"RequestCode"`
    }{
        RequestCode: 16,
    }

    jsonData, err := json.Marshal(request)
    if err != nil {
        return fmt.Errorf("failed to marshal disconnect request: %w", err)
    }

    err = mf.ws.WriteMessage(websocket.TextMessage, jsonData)
    if err != nil {
        return fmt.Errorf("failed to send disconnect request: %w", err)
    }

    return mf.Disconnect()
}

