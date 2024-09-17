package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to signal reconnection
	reconnect := make(chan struct{})

	go func() {
		for {
			err := mf.Connect(ctx)
			if err != nil {
				log.Printf("Failed to connect to market feed: %v", err)
				time.Sleep(5 * time.Second) // Wait before retrying
				continue
			}

			// Set up a timer to disconnect after 10 seconds
			timer := time.NewTimer(10 * time.Second)
			<-timer.C

			log.Println("Disconnecting after 10 seconds")
			err = mf.Disconnect()
			if err != nil {
				log.Printf("Error disconnecting: %v", err)
			}

			log.Println("Waiting 5 seconds before reconnecting...")
			time.Sleep(5 * time.Second)

			select {
			case <-ctx.Done():
				return
			case reconnect <- struct{}{}:
				// Signal for reconnection
			}
		}
	}()

	// Wait for a signal to exit (e.g., Ctrl+C)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-c:
			log.Println("Received interrupt signal, exiting...")
			cancel()
			return
		case <-reconnect:
			log.Println("Reconnecting...")
		}
	}
}