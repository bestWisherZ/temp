package main

import "testing"

func TestShardRPCHost(t *testing.T) {
	rt := Runtime{Config: Config{ShardRPCHosts: map[string]string{
		"bridge": "10.16.8.3", "business-1": "10.16.8.7",
	}}, Server: ServerConfig{RPCHost: "127.0.0.1"}}
	for shard, want := range map[string]string{"bridge": "10.16.8.3", "business-1": "10.16.8.7", "business-2": "127.0.0.1"} {
		if got := shardRPCHost(rt, shard); got != want {
			t.Fatalf("%s: got %s want %s", shard, got, want)
		}
	}
}
