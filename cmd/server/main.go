// Command server runs the v2 channel-home (holds truth; serves gateway).
package main

import (
	"context"
	"flag"
	"log"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/server"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	ch := flag.String("channel", "default", "channel id")
	db := flag.String("db", "channel.db", "channel sqlite path")
	flag.Parse()

	if err := server.Run(context.Background(), server.Config{
		ChannelID:  channel.ID(*ch),
		DBPath:     *db,
		ListenAddr: *addr,
	}); err != nil {
		log.Fatalf("server: %v", err)
	}
}
