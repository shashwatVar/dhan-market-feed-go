package marketfeed

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
)

type TickerPacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    SecurityID       uint32
    LastTradedPrice  float32
    LastTradeTime    uint32
}

type MarketDepthPacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    SecurityID       uint32
    LastTradedPrice  float32
    Depth            [5]DepthLevel
}

type DepthLevel struct {
    BidQuantity uint32
    AskQuantity uint32
    BidOrders   uint16
    AskOrders   uint16
    BidPrice    float32
    AskPrice    float32
}

type QuotePacket struct {
    FeedResponseCode   byte
    MessageLength      uint16
    ExchangeSegment    byte
    SecurityID         uint32
    LastTradedPrice    float32
    LastTradedQuantity uint16
    LastTradeTime      uint32
    AveragePrice       float32
    Volume             uint32
    TotalSellQuantity  uint32
    TotalBuyQuantity   uint32
    Open               float32
    Close              float32
    High               float32
    Low                float32
}

type OIPacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    SecurityID       uint32
    OpenInterest     uint32
}

type PreviousClosePacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    SecurityID       uint32
    PreviousClose    float32
    PreviousOI       uint32
}

type MarketStatusPacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    MarketStatus     uint32
}

type ServerDisconnectionPacket struct {
    FeedResponseCode byte
    MessageLength    uint16
    ExchangeSegment  byte
    SecurityID       uint32
    DisconnectionCode uint16
}


func (mf *MarketFeed) ProcessData(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            _, message, err := mf.ws.ReadMessage()
            if err != nil {
                if mf.onClose != nil {
                    _ = mf.onClose(err)
                }
                return
            }
            

            if len(message) > 0 {
                var packet interface{}
                var parseErr error

                switch message[0] {
                case 2:
                    packet, parseErr = parseTickerPacket(message)
                case 3:
                    packet, parseErr = parseMarketDepthPacket(message)
                case 4:
                    packet, parseErr = parseQuotePacket(message)
                case 5:
                    packet, parseErr = parseOIPacket(message)
				case 6:
                    packet, parseErr = parsePreviousClosePacket(message)
                case 7:
                    packet, parseErr = parseMarketStatusPacket(message)
                case 50:
                    packet, parseErr = parseServerDisconnectionPacket(message)
                default:
                    log.Printf("Unknown packet type: %d", message[0])
                    continue
                }

                if parseErr != nil {
                    log.Printf("Error parsing packet: %v", parseErr)
                } else {
					if mf.onMessage != nil {
						if err := mf.onMessage(packet); err != nil {
							log.Printf("Error in onMessage callback: %v", err)
						}
					}
                }
            }
        }
    }
}

func parseTickerPacket(message []byte) (TickerPacket, error) {
    if len(message) < 16 {
        return TickerPacket{}, fmt.Errorf("message too short")
    }

    packet := TickerPacket{
        FeedResponseCode: message[0],
        MessageLength:    binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:  message[3],
        SecurityID:       binary.LittleEndian.Uint32(message[4:8]),
        LastTradedPrice:  math.Float32frombits(binary.LittleEndian.Uint32(message[8:12])),
        LastTradeTime:    binary.LittleEndian.Uint32(message[12:16]),
    }

    return packet, nil
}

func parseMarketDepthPacket(message []byte) (MarketDepthPacket, error) {
    if len(message) < 112 {
        return MarketDepthPacket{}, fmt.Errorf("message too short")
    }

    packet := MarketDepthPacket{
        FeedResponseCode: message[0],
        MessageLength:    binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:  message[3],
        SecurityID:       binary.LittleEndian.Uint32(message[4:8]),
        LastTradedPrice:  math.Float32frombits(binary.LittleEndian.Uint32(message[8:12])),
    }

    for i := 0; i < 5; i++ {
        offset := 12 + i*24
        packet.Depth[i] = DepthLevel{
            BidQuantity: binary.LittleEndian.Uint32(message[offset : offset+4]),
            AskQuantity: binary.LittleEndian.Uint32(message[offset+4 : offset+8]),
            BidOrders:   binary.LittleEndian.Uint16(message[offset+8 : offset+10]),
            AskOrders:   binary.LittleEndian.Uint16(message[offset+10 : offset+12]),
            BidPrice:    math.Float32frombits(binary.LittleEndian.Uint32(message[offset+12 : offset+16])),
            AskPrice:    math.Float32frombits(binary.LittleEndian.Uint32(message[offset+16 : offset+20])),
        }
    }

    return packet, nil
}

func parseQuotePacket(message []byte) (QuotePacket, error) {
    if len(message) < 50 {
        return QuotePacket{}, fmt.Errorf("message too short")
    }

    packet := QuotePacket{
        FeedResponseCode:   message[0],
        MessageLength:      binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:    message[3],
        SecurityID:         binary.LittleEndian.Uint32(message[4:8]),
        LastTradedPrice:    math.Float32frombits(binary.LittleEndian.Uint32(message[8:12])),
        LastTradedQuantity: binary.LittleEndian.Uint16(message[12:14]),
        LastTradeTime:      binary.LittleEndian.Uint32(message[14:18]),
        AveragePrice:       math.Float32frombits(binary.LittleEndian.Uint32(message[18:22])),
        Volume:             binary.LittleEndian.Uint32(message[22:26]),
        TotalSellQuantity:  binary.LittleEndian.Uint32(message[26:30]),
        TotalBuyQuantity:   binary.LittleEndian.Uint32(message[30:34]),
        Open:               math.Float32frombits(binary.LittleEndian.Uint32(message[34:38])),
        Close:              math.Float32frombits(binary.LittleEndian.Uint32(message[38:42])),
        High:               math.Float32frombits(binary.LittleEndian.Uint32(message[42:46])),
        Low:                math.Float32frombits(binary.LittleEndian.Uint32(message[46:50])),
    }

    return packet, nil
}

func parseOIPacket(message []byte) (OIPacket, error) {
    if len(message) < 12 {
        return OIPacket{}, fmt.Errorf("message too short")
    }

    packet := OIPacket{
        FeedResponseCode: message[0],
        MessageLength:    binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:  message[3],
        SecurityID:       binary.LittleEndian.Uint32(message[4:8]),
        OpenInterest:     binary.LittleEndian.Uint32(message[8:12]),
    }

    return packet, nil
}

func parsePreviousClosePacket(message []byte) (PreviousClosePacket, error) {
    if len(message) < 16 {
        return PreviousClosePacket{}, fmt.Errorf("message too short")
    }

    packet := PreviousClosePacket{
        FeedResponseCode: message[0],
        MessageLength:    binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:  message[3],
        SecurityID:       binary.LittleEndian.Uint32(message[4:8]),
        PreviousClose:    math.Float32frombits(binary.LittleEndian.Uint32(message[8:12])),
        PreviousOI:       binary.LittleEndian.Uint32(message[12:16]),
    }

    return packet, nil
}

func parseMarketStatusPacket(message []byte) (MarketStatusPacket, error) {
    if len(message) < 8 {
        return MarketStatusPacket{}, fmt.Errorf("message too short")
    }

    packet := MarketStatusPacket{
        FeedResponseCode: message[0],
        MessageLength:    binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:  message[3],
        MarketStatus:     binary.LittleEndian.Uint32(message[4:8]),
    }

    return packet, nil
}

func parseServerDisconnectionPacket(message []byte) (ServerDisconnectionPacket, error) {
    if len(message) < 10 {
        return ServerDisconnectionPacket{}, fmt.Errorf("message too short")
    }

    packet := ServerDisconnectionPacket{
        FeedResponseCode:   message[0],
        MessageLength:      binary.LittleEndian.Uint16(message[1:3]),
        ExchangeSegment:    message[3],
        SecurityID:         binary.LittleEndian.Uint32(message[4:8]),
        DisconnectionCode:  binary.LittleEndian.Uint16(message[8:10]),
    }

    return packet, nil
}
