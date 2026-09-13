package main

import (
	"testing"
	"time"

	pbcommon "chainmaker.org/chainmaker/pb-go/v2/common"
)

func TestBlockCommitCompleteOverridesSchedulerFallback(t *testing.T) {
	key := blockTimingKey{Shard: "business-1", Height: 11}
	timings := make(map[blockTimingKey]blockTiming)
	fallbackStart := time.UnixMilli(1_000)
	metadataStart := time.UnixMilli(1_100)
	fallbackCommit := time.UnixMilli(1_250)
	localCommit := time.UnixMilli(1_300)

	setBlockTimingStart(timings, key, fallbackStart, "sched_metric_allow")
	setBlockTimingCommit(timings, key, fallbackCommit, "sched_metric")
	setBlockTimingStart(timings, key, metadataStart, blockMetadataStartSource)
	setBlockTimingCommit(timings, key, localCommit, localCommitCompleteSource)

	got := timings[key]
	if !got.AllowAt.Equal(metadataStart) || got.StartSource != blockMetadataStartSource {
		t.Fatalf("start = (%v, %q), want (%v, %q)",
			got.AllowAt, got.StartSource, metadataStart, blockMetadataStartSource)
	}
	if !got.CommitAt.Equal(localCommit) || got.CommitSource != localCommitCompleteSource {
		t.Fatalf("commit = (%v, %q), want (%v, %q)",
			got.CommitAt, got.CommitSource, localCommit, localCommitCompleteSource)
	}
	if got.DurationMillis != 200 {
		t.Fatalf("duration = %dms, want 200ms", got.DurationMillis)
	}
	if gotSource := blockTimingSource(got); gotSource != blockMetadataStartSource+"->"+localCommitCompleteSource {
		t.Fatalf("source = %q, want authoritative metadata source", gotSource)
	}
}

func TestSchedulerFallbackCannotOverwriteBlockCommitComplete(t *testing.T) {
	key := blockTimingKey{Shard: "bridge", Height: 17}
	timings := make(map[blockTimingKey]blockTiming)
	metadataStart := time.UnixMilli(2_000)
	localCommit := time.UnixMilli(2_400)

	setBlockTimingStart(timings, key, metadataStart, blockMetadataStartSource)
	setBlockTimingCommit(timings, key, localCommit, localCommitCompleteSource)
	setBlockTimingStart(timings, key, time.UnixMilli(1_500), "sched_metric_allow")
	setBlockTimingCommit(timings, key, time.UnixMilli(2_500), "sched_metric")

	got := timings[key]
	if !got.AllowAt.Equal(metadataStart) || !got.CommitAt.Equal(localCommit) {
		t.Fatalf("authoritative timing was overwritten: start=%v commit=%v", got.AllowAt, got.CommitAt)
	}
	if got.DurationMillis != 400 {
		t.Fatalf("duration = %dms, want 400ms", got.DurationMillis)
	}
}

func TestNormalizeExperimentDFATransfer(t *testing.T) {
	tx := WorkloadTx{
		Contract:  standardDFAExperimentContract,
		Method:    "TransferExperiment",
		From:      "1111111111111111111111111111111111111111",
		To:        "2222222222222222222222222222222222222222",
		FromShard: "business-3",
		ToShard:   "business-9",
	}
	if err := normalizeTx(&tx, 32, standardDFAExperimentContract); err != nil {
		t.Fatalf("normalizeTx() error = %v", err)
	}
	if tx.TxType != "cross" {
		t.Fatalf("tx type = %q, want cross", tx.TxType)
	}
}

func TestNormalizeRejectsProfileMismatch(t *testing.T) {
	tx := WorkloadTx{
		Contract: standardDFAContract,
		Method:   "Transfer",
		From:     "1111111111111111111111111111111111111111",
		To:       "2222222222222222222222222222222222222222",
	}
	if err := normalizeTx(&tx, 32, standardDFAExperimentContract); err == nil {
		t.Fatal("normalizeTx() accepted a dataset for the wrong contract profile")
	}
}

func TestClassifyExperimentDFATransfer(t *testing.T) {
	payload := &pbcommon.Payload{
		ContractName: standardDFAExperimentContract,
		Method:       "invoke_contract",
		Parameters: []*pbcommon.KeyValuePair{
			{Key: "method", Value: []byte("TransferExperiment")},
			{Key: "from_shard", Value: []byte("business-3")},
			{Key: "to_shard", Value: []byte("business-9")},
			{Key: "__cross_shard", Value: []byte("true")},
		},
	}
	isUser, isCross := classifyPayload(payload)
	if !isUser || !isCross {
		t.Fatalf("classifyPayload() = (%v, %v), want (true, true)", isUser, isCross)
	}
}

func TestEffectiveSendConcurrency(t *testing.T) {
	tests := []struct {
		name       string
		rate       int
		configured int
		want       int
	}{
		{name: "target rate raises concurrency", rate: 70000, configured: 512, want: 7000},
		{name: "configured minimum is retained", rate: 1000, configured: 512, want: 512},
		{name: "concurrency is bounded", rate: 500000, configured: 512, want: 16384},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveSendConcurrency(tt.rate, tt.configured); got != tt.want {
				t.Fatalf("effectiveSendConcurrency(%d, %d) = %d, want %d",
					tt.rate, tt.configured, got, tt.want)
			}
		})
	}
}
