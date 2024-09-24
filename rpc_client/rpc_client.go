package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type DataSourceArgs struct {
	SourceName string
	Options    interface{} // Adjust based on your DataSourceOptions structure
}

type DataSourceReply struct {
	Message string
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:9999")
	if err != nil {
		log.Fatalf("Error dialing RPC server: %v", err)
	}
	defer client.Close()

	// Example: Turn off a data source
	var reply DataSourceReply
	err = client.Call("RPCManager.TurnOnDataSource", "kucoin", &reply)
	if err != nil {
		log.Fatalf("Error calling RPC method: %v", err)
	}
	fmt.Println(reply.Message)
}
