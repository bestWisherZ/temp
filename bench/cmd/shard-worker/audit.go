package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pbcommon "chainmaker.org/chainmaker/pb-go/v2/common"
)

func runAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	config := fs.String("config", "config.json", "config")
	server := fs.String("server", "sender", "sender")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rt, err := loadRuntime(*config, *server)
	if err != nil {
		return err
	}
	clients, err := createClientSet(rt)
	if err != nil {
		return err
	}
	defer clients.Close()
	results, err := readTransactionsCSV(filepath.Join(rt.Config.WorkDir, "transactions.csv"))
	if err != nil {
		return err
	}
	blocks, err := readBlockRowsJSONL(filepath.Join(rt.Config.WorkDir, "blocks.jsonl"))
	if err != nil {
		return err
	}
	wanted := make(map[string]bool)
	for _, tx := range results {
		if tx.TxID != "" {
			wanted[tx.TxID] = true
		}
	}
	counts := map[string]int{"success": 0, "failed": 0, "unknown": 0}
	failures := make(map[string]string)
	seen := make(map[string]bool)
	for _, row := range blocks {
		client, err := clients.Client(row.Shard)
		if err != nil {
			return err
		}
		block, err := client.GetBlockByHeight(row.Height, false)
		if err != nil {
			return err
		}
		if block == nil || block.Block == nil {
			return fmt.Errorf("missing block %s/%d", row.Shard, row.Height)
		}
		for _, tx := range block.Block.Txs {
			if tx == nil || tx.Payload == nil {
				continue
			}
			id := tx.Payload.TxId
			if !wanted[id] || seen[id] {
				continue
			}
			seen[id] = true
			switch {
			case tx.Result == nil || tx.Result.ContractResult == nil:
				counts["unknown"]++
			case tx.Result.Code != pbcommon.TxStatusCode_SUCCESS || tx.Result.ContractResult.Code != 0:
				counts["failed"]++
				if len(failures) < 20 {
					failures[id] = tx.Result.String()
				}
			default:
				counts["success"]++
			}
		}
	}
	workload, err := loadWorkload(filepath.Join(rt.Config.WorkDir, "workload/transactions_all.jsonl"), rt.Config.BusinessShards, workloadContract(rt))
	if err != nil {
		return err
	}
	samples := make([]map[string]interface{}, 0)
	deadline := time.Now().Add(time.Minute)
	for _, tx := range workload {
		if tx.TxType != "cross" || len(samples) >= 10 {
			continue
		}
		amount, _ := tx.Amount.Int64()
		for _, account := range []struct {
			shard, address string
			want           int64
		}{
			{tx.FromShard, tx.From, rt.Config.InitialBalance - amount}, {tx.ToShard, tx.To, amount},
		} {
			client, err := clients.Client(account.shard)
			if err != nil {
				return err
			}
			got := ""
			for {
				resp, queryErr := client.QueryContract(tx.Contract, "BalanceOf", []*pbcommon.KeyValuePair{{Key: "account", Value: []byte(account.address)}}, 10)
				if queryErr != nil {
					return queryErr
				}
				if resp == nil || resp.ContractResult == nil {
					return fmt.Errorf("missing balance response")
				}
				if resp.Code != pbcommon.TxStatusCode_SUCCESS || resp.ContractResult.Code != 0 {
					return fmt.Errorf("balance query failed: %s", resp.String())
				}
				got = strings.TrimSpace(string(resp.ContractResult.Result))
				if got == strconv.FormatInt(account.want, 10) || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Second)
			}
			samples = append(samples, map[string]interface{}{"shard": account.shard, "account": account.address, "expected": account.want, "actual": got, "ok": got == strconv.FormatInt(account.want, 10)})
		}
	}
	report := map[string]interface{}{"execution": counts, "missing_from_blocks": len(wanted) - len(seen), "failure_samples": failures, "cross_balance_samples": samples}
	if err := writeJSON(filepath.Join(rt.Config.WorkDir, "audit.json"), report); err != nil {
		return err
	}
	fmt.Printf("execution audit: success=%d failed=%d unknown=%d missing=%d\n", counts["success"], counts["failed"], counts["unknown"], len(wanted)-len(seen))
	if counts["failed"] != 0 || counts["unknown"] != 0 || len(wanted) != len(seen) {
		return fmt.Errorf("execution audit failed; see audit.json")
	}
	for _, sample := range samples {
		if sample["ok"] != true {
			return fmt.Errorf("cross-shard balance audit failed; see audit.json")
		}
	}
	return nil
}
