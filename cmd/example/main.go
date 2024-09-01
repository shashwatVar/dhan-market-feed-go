package main

import (
	"context"
	"log"

	"github.com/shashwatVar/dhan-market-feed-go/pkg/marketfeed"
)

func main() {
	clientID := "client_id"
	accessToken := "access_token"

	instruments := []marketfeed.Instrument{
		{
			ExchangeSegment: 2,
			SecurityID:      "35075",
			FeedType:        15,
		},
	}

	onConnect := func() error {
		log.Println("Connected to market feed")
		return nil
	}

	onMessage := func(data interface{}) error {
		log.Printf("Received message: %v", data)
		return nil
	}

	onClose := func(err error) error {
		log.Printf("Connection closed: %v", err)
		return nil
	}

	mf := marketfeed.NewMarketFeed(clientID, accessToken, instruments, onConnect, onMessage, onClose)

	ctx := context.Background()
	err := mf.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to market feed: %v", err)
	}

	select {}
}