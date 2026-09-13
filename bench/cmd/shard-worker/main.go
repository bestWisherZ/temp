package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pbcommon "chainmaker.org/chainmaker/pb-go/v2/common"
	sdk "chainmaker.org/chainmaker/sdk-go/v2"
	sdkutils "chainmaker.org/chainmaker/sdk-go/v2/utils"
)

const (
	defaultOrgPattern             = "wx-org%d.chainmaker.org"
	defaultVersion                = "v1.0.0"
	tokenContract                 = "token_business"
	standardDFAContract           = "standard_dfa"
	standardDFAExperimentContract = "standard_dfa_experiment"
)

type Config struct {
	ChainmakerDir             string            `json:"chainmaker_dir"`
	BusinessShards            int               `json:"business_shards"`
	NodesPerShard             int               `json:"nodes_per_shard"`
	BaseRPCPort               int               `json:"base_rpc_port"`
	ShardRPCHosts             map[string]string `json:"shard_rpc_hosts"`
	MetricsCommand            []string          `json:"metrics_command"`
	NodeConnCnt               int               `json:"node_conn_cnt"`
	CoordinatorServer         string            `json:"coordinator_server"`
	Servers                   []ServerConfig    `json:"servers"`
	TxRate                    int               `json:"tx_rate"`
	TxCount                   int               `json:"tx_count"`
	Connections               int               `json:"connections"`
	IntraConsensusRounds      int               `json:"intra_consensus_rounds"`
	InterConsensusRounds      int               `json:"inter_consensus_rounds"`
	ScheduleStartHeight       uint64            `json:"schedule_start_height"`
	Amount                    int64             `json:"amount"`
	InitialBalance            int64             `json:"initial_balance"`
	InitAccounts              bool              `json:"init_accounts"`
	TxTimeoutSeconds          int64             `json:"tx_timeout_seconds"`
	DeployTimeoutSeconds      int64             `json:"deploy_timeout_seconds"`
	DeployConfirmWaitSeconds  int               `json:"deploy_confirm_wait_seconds"`
	ConfirmWaitSeconds        int               `json:"confirm_wait_seconds"`
	HeightWaitSeconds         int               `json:"height_wait_seconds"`
	PrepareTargetHeight       uint64            `json:"prepare_target_height"`
	PrepareConcurrency        int               `json:"prepare_concurrency"`
	RegisterConcurrency       int               `json:"register_concurrency"`
	WorkDir                   string            `json:"work_dir"`
	WorkloadContract          string            `json:"workload_contract"`
	TokenBusinessContractPath string            `json:"token_business_contract_path"`
	StandardDFAContractPath   string            `json:"standard_dfa_contract_path"`
	StandardDFAExperimentPath string            `json:"standard_dfa_experiment_contract_path"`
	PokeBlockContractPath     string            `json:"poke_block_contract_path"`
	HeartbeatContractPath     string            `json:"heartbeat_contract_path"`
}

type ServerConfig struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	NodeID    int    `json:"node_id"`
	RPCHost   string `json:"rpc_host"`
	RemoteDir string `json:"remote_dir"`
}

type Runtime struct {
	Root   string
	Config Config
	Server ServerConfig
}

type Shard struct {
	Key   string
	Type  string
	ID    int
	Label string
}

type ShardClient struct {
	Shard  Shard
	Client *sdk.ChainClient
}

type ClientSet struct {
	clients map[string]*ShardClient
}

type AdminUser struct {
	Name        string
	OrgID       string
	SignKeyPath string
	SignCrtPath string
}

type WorkloadTx struct {
	Seq          int64       `json:"seq"`
	WorkloadTxID string      `json:"workload_tx_id"`
	Contract     string      `json:"contract"`
	Method       string      `json:"method"`
	From         string      `json:"from"`
	To           string      `json:"to"`
	Amount       json.Number `json:"amount"`
	FromShard    string      `json:"from_shard"`
	ToShard      string      `json:"to_shard"`
	TxType       string      `json:"tx_type"`
	Server       string      `json:"server"`
}

type SubmitResult struct {
	Seq              int64     `json:"seq"`
	TxID             string    `json:"tx_id,omitempty"`
	TargetShard      string    `json:"target_shard"`
	TxType           string    `json:"tx_type"`
	StartedAt        time.Time `json:"started_at"`
	SubmittedAt      time.Time `json:"submitted_at"`
	Error            string    `json:"error,omitempty"`
	Confirmed        bool      `json:"confirmed"`
	ConfirmedAt      time.Time `json:"confirmed_at,omitempty"`
	ConfirmLatencyMs int64     `json:"confirm_latency_ms,omitempty"`
	BlockShard       string    `json:"block_shard,omitempty"`
	BlockHeight      uint64    `json:"block_height,omitempty"`
	BlockTimestamp   int64     `json:"block_timestamp,omitempty"`
	BlockHash        string    `json:"block_hash,omitempty"`
}

type ServerResult struct {
	Server             string         `json:"server"`
	Host               string         `json:"host"`
	NodeID             int            `json:"node_id"`
	RPCHost            string         `json:"rpc_host"`
	Dataset            string         `json:"dataset"`
	WorkloadContract   string         `json:"workload_contract"`
	WorkloadMethod     string         `json:"workload_method"`
	ConfirmWaitSeconds int            `json:"confirm_wait_seconds"`
	Attempted          int            `json:"attempted"`
	Submitted          int64          `json:"submitted"`
	SubmitFailed       int64          `json:"submit_failed"`
	Confirmed          int            `json:"confirmed"`
	Missing            int            `json:"missing"`
	TPS                float64        `json:"tps"`
	PeakTPS            float64        `json:"peak_tps"`
	AvgLatencyMs       float64        `json:"avg_latency_ms"`
	MaxLatencyMs       int64          `json:"max_latency_ms"`
	StartedAt          string         `json:"started_at"`
	SendFinishedAt     string         `json:"send_finished_at"`
	MeasureFinishedAt  string         `json:"measure_finished_at"`
	BlocksCSV          string         `json:"blocks_csv"`
	PhaseSyncCSV       string         `json:"phase_sync_csv,omitempty"`
	ShardBlocksDir     string         `json:"shard_blocks_dir,omitempty"`
	TransactionsCSV    string         `json:"transactions_csv"`
	FailureReasons     map[string]int `json:"failure_reasons"`
}

type MetricsManifest struct {
	Server        string `json:"server"`
	Host          string `json:"host"`
	NodeID        int    `json:"node_id"`
	RPCHost       string `json:"rpc_host"`
	GeneratedAt   string `json:"generated_at"`
	Since         string `json:"since,omitempty"`
	Until         string `json:"until,omitempty"`
	EventCount    int    `json:"event_count"`
	EventsJSONL   string `json:"events_jsonl"`
	ChainmakerDir string `json:"chainmaker_dir"`
}

type Snapshot struct {
	CapturedAt     string            `json:"captured_at"`
	ChainmakerDir  string            `json:"chainmaker_dir"`
	BusinessShards int               `json:"business_shards"`
	NodeID         int               `json:"node_id"`
	RPCHost        string            `json:"rpc_host"`
	Heights        map[string]uint64 `json:"heights"`
}

type BlockRow struct {
	Shard             string `json:"shard"`
	Height            uint64 `json:"height"`
	BlockHash         string `json:"block_hash"`
	Timestamp         int64  `json:"block_timestamp"`
	StartTimestampMs  int64  `json:"start_timestamp_millis,omitempty"`
	StartSource       string `json:"start_source,omitempty"`
	SpecialBlock      bool   `json:"special_block,omitempty"`
	SchedulerCommit   bool   `json:"scheduler_commit,omitempty"`
	ConsensusStartMs  int64  `json:"consensus_start_millis,omitempty"`
	CommitTimestampMs int64  `json:"commit_timestamp_millis,omitempty"`
	BlockTimeMillis   int64  `json:"block_time_millis"`
	BlockTimeSource   string `json:"block_time_source,omitempty"`
	IntervalCommitMs  int64  `json:"-"`
	BlockIntervalMs   int64  `json:"block_interval_millis"`
	BlockIntervalSrc  string `json:"block_interval_source,omitempty"`
	PhaseSyncMillis   int64  `json:"phase_sync_millis"`
	TotalTxCount      uint64 `json:"total_tx_count"`
	UserTxCount       uint64 `json:"user_tx_count"`
	CrossUserTxCount  uint64 `json:"cross_user_tx_count"`
	IntraUserTxCount  uint64 `json:"intra_user_tx_count"`
	InternalTxCount   uint64 `json:"internal_tx_count"`
	CumulativeTotalTx uint64 `json:"cumulative_total_tx_count"`
	CumulativeUserTx  uint64 `json:"cumulative_user_tx_count"`
	ElapsedMillis     int64  `json:"elapsed_millis_from_first"`
}

type ShardSummary struct {
	StartHeight       uint64  `json:"start_height"`
	EndHeight         uint64  `json:"end_height"`
	BlockCount        uint64  `json:"block_count"`
	TotalTxCount      uint64  `json:"total_tx_count"`
	UserTxCount       uint64  `json:"user_tx_count"`
	CrossUserTxCount  uint64  `json:"cross_user_tx_count"`
	IntraUserTxCount  uint64  `json:"intra_user_tx_count"`
	InternalTxCount   uint64  `json:"internal_tx_count"`
	FirstTimestamp    int64   `json:"first_block_timestamp"`
	LastTimestamp     int64   `json:"last_block_timestamp"`
	DurationMillis    int64   `json:"duration_millis"`
	CommittedTPSTotal float64 `json:"committed_tps_total"`
	CommittedTPSUser  float64 `json:"committed_tps_user"`
}

type ReportResult struct {
	GeneratedAt        string                  `json:"generated_at"`
	StartSnapshot      string                  `json:"start_snapshot"`
	EndSnapshot        string                  `json:"end_snapshot"`
	BusinessShards     int                     `json:"business_shards"`
	NodeID             int                     `json:"node_id"`
	RPCHost            string                  `json:"rpc_host"`
	BlockCount         uint64                  `json:"block_count"`
	DurationMillis     int64                   `json:"duration_millis"`
	TotalTxCount       uint64                  `json:"total_tx_count"`
	UserTxCount        uint64                  `json:"user_tx_count"`
	CrossUserTxCount   uint64                  `json:"cross_user_tx_count"`
	IntraUserTxCount   uint64                  `json:"intra_user_tx_count"`
	InternalTxCount    uint64                  `json:"internal_tx_count"`
	CommittedTPSTotal  float64                 `json:"committed_tps_total"`
	CommittedTPSUser   float64                 `json:"committed_tps_user"`
	AverageBlockTxs    float64                 `json:"average_block_txs"`
	AverageBlockUserTx float64                 `json:"average_block_user_txs"`
	PerShard           map[string]ShardSummary `json:"per_shard"`
}

type PerformanceMetrics struct {
	TPS          float64
	PeakTPS      float64
	AvgLatencyMs float64
	MaxLatencyMs int64
}

type periodPerfBucket struct {
	Index        uint64
	TxCount      uint64
	StartBlockTs int64
	EndBlockTs   int64
	HasBusiness  bool
	HasBridge    bool
}

type confirmedPeriodBucket struct {
	TxCount     uint64
	Start       time.Time
	End         time.Time
	HasBusiness bool
	HasBridge   bool
}

type blockTimingKey struct {
	Shard  string
	Height uint64
}

type blockTiming struct {
	AllowAt          time.Time
	StartSource      string
	ConsensusStartAt time.Time
	CommitAt         time.Time
	IntervalCommitAt time.Time
	DurationMillis   int64
	CommitSource     string
	Special          bool
}

type scheduleMetricEvent struct {
	Server string            `json:"server,omitempty"`
	Host   string            `json:"host,omitempty"`
	NodeID int               `json:"node_id,omitempty"`
	Shard  string            `json:"shard"`
	Event  string            `json:"event"`
	At     time.Time         `json:"at"`
	Fields map[string]string `json:"fields"`
}

const schedMetricPrefix = "[SCHED_METRIC]"

const (
	blockMetadataStartSource  = "block_metadata_start"
	localCommitCompleteSource = "local_commit_complete"
)

var commitBlockLogRe = regexp.MustCompile(`commit block \[([0-9]+)\]`)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "prepare":
		err = runPrepare(os.Args[2:])
	case "run":
		err = runBenchmark(os.Args[2:])
	case "snapshot":
		err = runSnapshot(os.Args[2:])
	case "csv":
		err = runCSV(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  go run ./cmd/shard-worker prepare  -config ./config.json
  go run ./cmd/shard-worker run      -config ./config.json -server server15 -dataset ./out/workload/transactions_all.jsonl
  go run ./cmd/shard-worker snapshot -config ./config.json -out ./out/snapshots/start.json
  go run ./cmd/shard-worker csv      -config ./config.json -start ./out/snapshots/start.json -end ./out/snapshots/end.json -out-csv ./out/blocks.csv -out-json ./out/result.json
`)
}

func runPrepare(args []string) error {
	fs := flag.NewFlagSet("prepare", flag.ExitOnError)
	configPath := fs.String("config", "./config.json", "config path")
	serverName := fs.String("server", "", "server name")
	outDir := fs.String("out-dir", "", "output dir")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rt, err := loadRuntime(*configPath, *serverName)
	if err != nil {
		return err
	}
	if *outDir == "" {
		*outDir = filepath.Join(rt.Config.WorkDir, "prepare")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	started := time.Now()
	clients, err := createClientSet(rt)
	if err != nil {
		return err
	}
	defer clients.Close()

	targetHeight := rt.Config.PrepareTargetHeight
	if targetHeight > 0 {
		fmt.Printf("\n>>> 步骤 1/4: 准备前检查 | 目标高度=%d\n", targetHeight)
		statuses := collectPrepareStatuses(rt, clients)
		printPrepareStatuses(statuses)
		if prepareReady(statuses, targetHeight) {
			fmt.Printf("\n✓ 准备完成！所有合约已部署，所有分片高度已经精确停在 %d\n", targetHeight)
			result := map[string]interface{}{
				"server":        rt.Server.Name,
				"node_id":       rt.Server.NodeID,
				"rpc_host":      rt.Server.RPCHost,
				"target_height": targetHeight,
				"already_ready": true,
				"started_at":    started.Format(time.RFC3339Nano),
				"finished_at":   time.Now().Format(time.RFC3339Nano),
				"elapsed_ms":    time.Since(started).Milliseconds(),
			}
			return writeJSON(filepath.Join(*outDir, "prepare_result.json"), result)
		}
		if err := validatePrepareCanProceed(statuses, targetHeight); err != nil {
			return err
		}
	}

	fmt.Println("\n>>> 步骤 2/4: 部署合约与初始化业务账户")
	if err := deployBridge(rt, clients); err != nil {
		return err
	}
	jobs := make([]namedJob, 0, rt.Config.BusinessShards)
	for shardID := 1; shardID <= rt.Config.BusinessShards; shardID++ {
		shardID := shardID
		jobs = append(jobs, namedJob{
			name: fmt.Sprintf("business-%d", shardID),
			run: func() error {
				return deployBusiness(rt, clients, shardID)
			},
		})
	}
	if err := runJobs("并行部署业务分片合约与初始化账户", jobs, rt.Config.PrepareConcurrency); err != nil {
		return err
	}

	if targetHeight > 0 {
		fmt.Printf("\n>>> 步骤 3/4: 部署后检查 | 目标高度=%d\n", targetHeight)
		statuses := collectPrepareStatuses(rt, clients)
		printPrepareStatuses(statuses)
		if err := validatePrepareCanProceed(statuses, targetHeight); err != nil {
			return err
		}

		fmt.Printf("\n>>> 步骤 4/4: 推进区块高度到 %d\n", targetHeight)
		printCurrentPrepareHeights(rt, clients)
		if err := advanceAllShards(rt, clients, targetHeight); err != nil {
			return err
		}

		fmt.Println("\n>>> 最终检查:")
		statuses = collectPrepareStatuses(rt, clients)
		printPrepareStatuses(statuses)
		if !prepareReady(statuses, targetHeight) {
			fmt.Println("\n⚠ 警告: 准备未达标，请根据上面的 missing/height 检查")
			return fmt.Errorf("prepare not ready after advance: target_height=%d", targetHeight)
		}
		fmt.Printf("\n✓ 准备完成！所有业务分片与桥接分片高度都精确停在 %d，相关合约已部署\n", targetHeight)
		fmt.Println("✓ 现在可以运行模式2测试分时共识功能")
	} else {
		fmt.Println("\n✓ 准备完成！相关合约已部署")
	}

	result := map[string]interface{}{
		"server":        rt.Server.Name,
		"node_id":       rt.Server.NodeID,
		"rpc_host":      rt.Server.RPCHost,
		"target_height": targetHeight,
		"already_ready": false,
		"started_at":    started.Format(time.RFC3339Nano),
		"finished_at":   time.Now().Format(time.RFC3339Nano),
		"elapsed_ms":    time.Since(started).Milliseconds(),
	}
	return writeJSON(filepath.Join(*outDir, "prepare_result.json"), result)
}

func runBenchmark(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "./config.json", "config path")
	serverName := fs.String("server", "", "server name")
	datasetPath := fs.String("dataset", "", "dataset JSONL path")
	txRate := fs.Int("tx-rate", 0, "server-local tx/s")
	connections := fs.Int("connections", 0, "worker goroutines")
	outDir := fs.String("out-dir", "", "output dir")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rt, err := loadRuntime(*configPath, *serverName)
	if err != nil {
		return err
	}
	rate := *txRate
	if rate <= 0 {
		rate = serverRate(rt.Config)
	}
	if rate <= 0 {
		rate = 1
	}
	workerCount := *connections
	if workerCount <= 0 {
		workerCount = rt.Config.Connections
	}
	if workerCount <= 0 {
		workerCount = 64
	}
	configuredWorkerCount := workerCount
	workerCount = effectiveSendConcurrency(rate, workerCount)
	if workerCount != configuredWorkerCount {
		fmt.Printf("===> Auto-scale senders for open-loop rate: configured=%d effective=%d target_rate=%d tx/s\n",
			configuredWorkerCount, workerCount, rate)
	}
	if *datasetPath == "" {
		*datasetPath = filepath.Join(rt.Config.WorkDir, "workload", "transactions_all.jsonl")
	}
	*datasetPath = resolvePath(rt.Root, *datasetPath)
	if *outDir == "" {
		*outDir = rt.Config.WorkDir
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	txs, err := loadWorkload(*datasetPath, rt.Config.BusinessShards, workloadContract(rt))
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return fmt.Errorf("dataset is empty: %s", *datasetPath)
	}
	clients, err := createClientSet(rt)
	if err != nil {
		return err
	}
	defer clients.Close()
	fmt.Printf("===> Start benchmark: server=%s contract=%s method=%s tx_count=%d tx_rate=%d connections=%d dataset=%s\n",
		rt.Server.Name, workloadContract(rt), txs[0].Method, len(txs), rate, workerCount, *datasetPath)
	targetShards := allShardKeys(rt.Config.BusinessShards)
	startHeights, err := captureHeights(clients, targetShards)
	if err != nil {
		return err
	}
	fmt.Printf("start heights: %s\n", formatShardHeights(targetShards, startHeights, 12))
	started := time.Now()
	results, failureReasons := sendWorkload(rt, clients, txs, rate, workerCount)
	finished := time.Now()
	fmt.Printf("send finished: submitted=%d failed=%d elapsed=%s\n",
		countSubmitted(results), countSubmitFailed(results), formatDuration(finished.Sub(started)))
	fmt.Printf("===> Wait confirmations: shards=%s timeout=%ds\n",
		strings.Join(targetShards, ","), rt.Config.ConfirmWaitSeconds)
	blockRows, confirmed := collectRunConfirmations(rt, clients, targetShards, startHeights, results)
	measureFinished := time.Now()
	if len(rt.Config.MetricsCommand) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		cmd := exec.CommandContext(ctx, rt.Config.MetricsCommand[0], rt.Config.MetricsCommand[1:]...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		err := cmd.Run()
		cancel()
		if err != nil {
			// Persist the measured transactions before reporting a collection failure.
			_ = writeTransactionsCSV(filepath.Join(*outDir, "transactions.csv"), results)
			_ = writeBlockRowsJSONL(filepath.Join(*outDir, "blocks.jsonl"), blockRows)
			return fmt.Errorf("collect metrics (raw measurements saved): %w", err)
		}
	}
	metricEvents := applyLocalScheduleMetricsForRun(rt, blockRows, results, started, measureFinished, targetShards)
	if metricEvents > 0 {
		fmt.Printf("applied local schedule metrics: server=%s events=%d\n", rt.Server.Name, metricEvents)
	} else {
		fmt.Printf("warn: no local schedule metrics found for this run; fallback block timing is used\n")
	}
	finalizeBlockTimingFields(blockRows)
	fillBlockRowIntervals(blockRows)
	chainDurationMs, committedTPS := runChainMetrics(blockRows)
	metrics := calculatePerformanceMetrics(rt, results, blockRows)
	var submitted int64
	var failed int64
	for _, result := range results {
		if result.TxID != "" && result.Error == "" {
			submitted++
		} else {
			failed++
		}
	}
	blocksCSV := filepath.Join(*outDir, "blocks.csv")
	phaseSyncCSV := filepath.Join(*outDir, "phase_sync.csv")
	blocksJSONL := filepath.Join(*outDir, "blocks.jsonl")
	shardBlocksDir := filepath.Join(*outDir, "shards")
	transactionsCSV := filepath.Join(*outDir, "transactions.csv")
	blockReport := reportFromRunRows(rt, blockRows, chainDurationMs, committedTPS)
	if err := writeBlockCSV(blocksCSV, blockRows, blockReport); err != nil {
		return err
	}
	if err := writePhaseSyncCSV(phaseSyncCSV, blockRows); err != nil {
		return err
	}
	if err := writeBlockRowsJSONL(blocksJSONL, blockRows); err != nil {
		return err
	}
	if err := writeShardBlockCSVs(shardBlocksDir, blockRows, targetShards, rt); err != nil {
		return err
	}
	if err := writeTransactionsCSV(transactionsCSV, results); err != nil {
		return err
	}
	serverResult := ServerResult{
		Server:             rt.Server.Name,
		Host:               rt.Server.Host,
		NodeID:             rt.Server.NodeID,
		RPCHost:            rt.Server.RPCHost,
		Dataset:            *datasetPath,
		WorkloadContract:   workloadContract(rt),
		WorkloadMethod:     txs[0].Method,
		ConfirmWaitSeconds: rt.Config.ConfirmWaitSeconds,
		Attempted:          len(results),
		Submitted:          submitted,
		SubmitFailed:       failed,
		Confirmed:          confirmed,
		Missing:            int(submitted) - confirmed,
		TPS:                metrics.TPS,
		PeakTPS:            metrics.PeakTPS,
		AvgLatencyMs:       metrics.AvgLatencyMs,
		MaxLatencyMs:       metrics.MaxLatencyMs,
		StartedAt:          started.Format(time.RFC3339Nano),
		SendFinishedAt:     finished.Format(time.RFC3339Nano),
		MeasureFinishedAt:  measureFinished.Format(time.RFC3339Nano),
		BlocksCSV:          blocksCSV,
		PhaseSyncCSV:       phaseSyncCSV,
		ShardBlocksDir:     shardBlocksDir,
		TransactionsCSV:    transactionsCSV,
		FailureReasons:     failureReasons,
	}
	resultPath := filepath.Join(*outDir, "result.json")
	if err := writeJSON(resultPath, serverResult); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(*outDir, "server_result.json"), serverResult); err != nil {
		return err
	}
	fmt.Printf("measure finished: confirmed=%d missing=%d tps=%.2f peak_tps=%.2f avg_latency_ms=%.2f max_latency_ms=%d blocks_csv=%s shard_blocks_dir=%s tx_csv=%s result=%s\n",
		confirmed, int(submitted)-confirmed, metrics.TPS, metrics.PeakTPS, metrics.AvgLatencyMs, metrics.MaxLatencyMs, blocksCSV, shardBlocksDir, transactionsCSV, resultPath)
	return nil
}

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	configPath := fs.String("config", "./config.json", "config path")
	serverName := fs.String("server", "", "server name")
	outPath := fs.String("out", "", "snapshot output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outPath == "" {
		return fmt.Errorf("-out is required")
	}
	rt, err := loadRuntime(*configPath, *serverName)
	if err != nil {
		return err
	}
	clients, err := createClientSet(rt)
	if err != nil {
		return err
	}
	defer clients.Close()
	heights := make(map[string]uint64, rt.Config.BusinessShards+1)
	for _, shard := range allShards(rt.Config.BusinessShards) {
		height, err := clients.Height(shard.Key)
		if err != nil {
			return err
		}
		heights[shard.Key] = height
	}
	snap := Snapshot{
		CapturedAt:     time.Now().Format(time.RFC3339Nano),
		ChainmakerDir:  rt.Config.ChainmakerDir,
		BusinessShards: rt.Config.BusinessShards,
		NodeID:         rt.Server.NodeID,
		RPCHost:        rt.Server.RPCHost,
		Heights:        heights,
	}
	if err := writeJSON(resolvePath(rt.Root, *outPath), snap); err != nil {
		return err
	}
	fmt.Printf("snapshot: %s shards=%d\n", *outPath, len(heights))
	return nil
}

func runCSV(args []string) error {
	fs := flag.NewFlagSet("csv", flag.ExitOnError)
	configPath := fs.String("config", "./config.json", "config path")
	serverName := fs.String("server", "", "server name")
	startPath := fs.String("start", "", "start snapshot")
	endPath := fs.String("end", "", "end snapshot")
	outCSV := fs.String("out-csv", "", "CSV output path")
	outJSON := fs.String("out-json", "", "JSON output path")
	includeEmpty := fs.Bool("include-empty", true, "include empty blocks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *startPath == "" || *endPath == "" || *outCSV == "" || *outJSON == "" {
		return fmt.Errorf("-start, -end, -out-csv and -out-json are required")
	}
	rt, err := loadRuntime(*configPath, *serverName)
	if err != nil {
		return err
	}
	start, err := readSnapshot(resolvePath(rt.Root, *startPath))
	if err != nil {
		return err
	}
	end, err := readSnapshot(resolvePath(rt.Root, *endPath))
	if err != nil {
		return err
	}
	clients, err := createClientSet(rt)
	if err != nil {
		return err
	}
	defer clients.Close()
	rows, report, err := collectBlocks(rt, clients, start, end, resolvePath(rt.Root, *startPath), resolvePath(rt.Root, *endPath), *includeEmpty)
	if err != nil {
		return err
	}
	finalizeBlockTimingFields(rows)
	fillBlockRowIntervals(rows)
	if err := writeBlockCSV(resolvePath(rt.Root, *outCSV), rows, report); err != nil {
		return err
	}
	if err := writeJSON(resolvePath(rt.Root, *outJSON), report); err != nil {
		return err
	}
	fmt.Printf("report: csv=%s json=%s blocks=%d total_txs=%d user_txs=%d user_tps=%.2f\n",
		*outCSV, *outJSON, report.BlockCount, report.TotalTxCount, report.UserTxCount, report.CommittedTPSUser)
	return nil
}

func loadRuntime(configPath, serverName string) (Runtime, error) {
	root, err := os.Getwd()
	if err != nil {
		return Runtime{}, err
	}
	configPath = resolvePath(root, configPath)
	cfg, err := loadConfig(configPath)
	if err != nil {
		return Runtime{}, err
	}
	cfg.ChainmakerDir = resolvePath(root, cfg.ChainmakerDir)
	cfg.WorkDir = resolvePath(root, cfg.WorkDir)
	if cfg.TokenBusinessContractPath == "" {
		cfg.TokenBusinessContractPath = filepath.Join(root, "contracts", "token_business", "token_business.7z")
	} else {
		cfg.TokenBusinessContractPath = resolvePath(root, cfg.TokenBusinessContractPath)
	}
	if cfg.StandardDFAContractPath == "" {
		cfg.StandardDFAContractPath = filepath.Join(root, "contracts", "standard_dfa", "standard_dfa.7z")
	} else {
		cfg.StandardDFAContractPath = resolvePath(root, cfg.StandardDFAContractPath)
	}
	if cfg.StandardDFAExperimentPath == "" {
		cfg.StandardDFAExperimentPath = filepath.Join(root, "contracts", "standard_dfa_experiment", "standard_dfa_experiment.7z")
	} else {
		cfg.StandardDFAExperimentPath = resolvePath(root, cfg.StandardDFAExperimentPath)
	}
	if cfg.PokeBlockContractPath == "" {
		cfg.PokeBlockContractPath = filepath.Join(root, "contracts", "poke_block", "poke_block.7z")
	} else {
		cfg.PokeBlockContractPath = resolvePath(root, cfg.PokeBlockContractPath)
	}
	if cfg.HeartbeatContractPath == "" {
		cfg.HeartbeatContractPath = filepath.Join(root, "contracts", "sharding_heartbeat", "sharding_heartbeat.7z")
	} else {
		cfg.HeartbeatContractPath = resolvePath(root, cfg.HeartbeatContractPath)
	}
	if serverName == "" {
		serverName = strings.TrimSpace(os.Getenv("SERVER_NAME"))
	}
	if serverName == "" {
		serverName = cfg.CoordinatorServer
	}
	if serverName == "" && len(cfg.Servers) > 0 {
		serverName = cfg.Servers[0].Name
	}
	server, err := findServer(cfg, serverName)
	if err != nil {
		return Runtime{}, err
	}
	return Runtime{Root: root, Config: cfg, Server: server}, nil
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.BusinessShards <= 0 {
		cfg.BusinessShards = 32
	}
	if cfg.NodesPerShard <= 0 {
		cfg.NodesPerShard = 4
	}
	if cfg.BaseRPCPort <= 0 {
		cfg.BaseRPCPort = 12301
	}
	if cfg.NodeConnCnt <= 0 {
		cfg.NodeConnCnt = 10
	}
	if cfg.TxTimeoutSeconds <= 0 {
		cfg.TxTimeoutSeconds = 60
	}
	if cfg.DeployTimeoutSeconds <= 0 {
		cfg.DeployTimeoutSeconds = 180
	}
	if cfg.DeployConfirmWaitSeconds <= 0 {
		cfg.DeployConfirmWaitSeconds = 300
	}
	if cfg.ConfirmWaitSeconds <= 0 {
		cfg.ConfirmWaitSeconds = 600
	}
	if cfg.HeightWaitSeconds <= 0 {
		cfg.HeightWaitSeconds = 60
	}
	if cfg.PrepareConcurrency <= 0 {
		cfg.PrepareConcurrency = 8
	}
	if cfg.RegisterConcurrency <= 0 {
		cfg.RegisterConcurrency = 8
	}
	if cfg.Connections <= 0 {
		cfg.Connections = 64
	}
	if cfg.IntraConsensusRounds <= 0 {
		cfg.IntraConsensusRounds = 5
	}
	if cfg.InterConsensusRounds <= 0 {
		cfg.InterConsensusRounds = 8
	}
	if cfg.ScheduleStartHeight == 0 {
		cfg.ScheduleStartHeight = cfg.PrepareTargetHeight
		if cfg.ScheduleStartHeight == 0 {
			cfg.ScheduleStartHeight = 10
		}
	}
	if cfg.InitialBalance <= 0 {
		cfg.InitialBalance = 1000
	}
	if cfg.Amount <= 0 {
		cfg.Amount = 1
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir = "./out"
	}
	cfg.WorkloadContract = strings.TrimSpace(cfg.WorkloadContract)
	if cfg.WorkloadContract == "" {
		cfg.WorkloadContract = tokenContract
	}
	if !isSupportedWorkloadContract(cfg.WorkloadContract) {
		return cfg, fmt.Errorf("unsupported workload_contract %q; supported: %s, %s, %s",
			cfg.WorkloadContract, tokenContract, standardDFAContract, standardDFAExperimentContract)
	}
	if cfg.ChainmakerDir == "" {
		return cfg, fmt.Errorf("chainmaker_dir is required")
	}
	return cfg, nil
}

func findServer(cfg Config, name string) (ServerConfig, error) {
	for _, server := range cfg.Servers {
		if server.Name == name {
			if server.NodeID <= 0 {
				server.NodeID = 1
			}
			if server.RPCHost == "" {
				server.RPCHost = "127.0.0.1"
			}
			return server, nil
		}
	}
	return ServerConfig{}, fmt.Errorf("server not found: %s", name)
}

func serverRate(cfg Config) int {
	if cfg.TxRate <= 0 {
		return 1
	}
	return cfg.TxRate
}

func allShards(businessShards int) []Shard {
	shards := make([]Shard, 0, businessShards+1)
	for i := 1; i <= businessShards; i++ {
		shards = append(shards, Shard{
			Key:   fmt.Sprintf("business-%d", i),
			Type:  "business",
			ID:    i,
			Label: fmt.Sprintf("business-%d", i),
		})
	}
	shards = append(shards, Shard{Key: "bridge", Type: "bridge", ID: 0, Label: "bridge"})
	return shards
}

func allShardKeys(businessShards int) []string {
	shards := allShards(businessShards)
	keys := make([]string, 0, len(shards))
	for _, shard := range shards {
		keys = append(keys, shard.Key)
	}
	return keys
}

func createClientSet(rt Runtime) (*ClientSet, error) {
	shards := allShards(rt.Config.BusinessShards)
	cs := &ClientSet{clients: make(map[string]*ShardClient, len(shards))}
	var mu sync.Mutex
	jobs := make([]namedJob, 0, len(shards))
	for _, shard := range shards {
		shard := shard
		jobs = append(jobs, namedJob{
			name: shard.Key,
			run: func() error {
				cc, err := createShardClient(rt, shard, "client1")
				if err != nil {
					return err
				}
				mu.Lock()
				cs.clients[shard.Key] = &ShardClient{Shard: shard, Client: cc}
				mu.Unlock()
				return nil
			},
		})
	}
	if err := runJobs("register shard clients", jobs, rt.Config.RegisterConcurrency); err != nil {
		cs.Close()
		return nil, err
	}
	return cs, nil
}

func createShardClient(rt Runtime, shard Shard, userType string) (*sdk.ChainClient, error) {
	confPath, err := generateSDKConfig(rt, shard, userType)
	if err != nil {
		return nil, err
	}
	cc, err := sdk.NewChainClient(sdk.WithConfPath(confPath))
	if err != nil {
		return nil, fmt.Errorf("new chain client %s: %w", shard.Key, err)
	}
	if _, err := cc.GetCurrentBlockHeight(); err != nil {
		_ = cc.Stop()
		return nil, fmt.Errorf("check connection %s: %w", shard.Key, err)
	}
	return cc, nil
}

func generateSDKConfig(rt Runtime, shard Shard, userType string) (string, error) {
	shardDir := shardDir(shard)
	chainID := shardChainID(shard)
	orgID := fmt.Sprintf(defaultOrgPattern, rt.Server.NodeID)
	cryptoPath := filepath.Join(rt.Config.ChainmakerDir, "build", shardDir, "crypto-config", orgID)
	rpcPort := shardRPCPort(rt.Config, shard, rt.Server.NodeID)
	tlsHostName := fmt.Sprintf("consensus1.tls.%s", orgID)
	if userType == "" {
		userType = "client1"
	}
	dir := filepath.Join(rt.Config.WorkDir, "sdk_configs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("sdk_%s_node%d_%s.yml", sanitizeFileName(shard.Key), rt.Server.NodeID, userType))
	content := fmt.Sprintf(`chain_client:
  chain_id: "%s"
  org_id: "%s"
  user_key_file_path: "%s/user/%s/%s.tls.key"
  user_crt_file_path: "%s/user/%s/%s.tls.crt"
  user_sign_key_file_path: "%s/user/%s/%s.sign.key"
  user_sign_crt_file_path: "%s/user/%s/%s.sign.crt"
  optimize_detection: -1
  nodes:
    - node_addr: "%s:%d"
      conn_cnt: %d
      enable_tls: true
      trust_root_paths:
        - "%s/ca"
      tls_host_name: "%s"
  rpc_client:
    max_receive_message_size: 100
    max_send_message_size: 100
`,
		chainID,
		orgID,
		cryptoPath, userType, userType,
		cryptoPath, userType, userType,
		cryptoPath, userType, userType,
		cryptoPath, userType, userType,
		shardRPCHost(rt, shard.Key), rpcPort, rt.Config.NodeConnCnt, cryptoPath, tlsHostName,
	)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func shardRPCHost(rt Runtime, shardKey string) string {
	if host := strings.TrimSpace(rt.Config.ShardRPCHosts[shardKey]); host != "" {
		return host
	}
	return rt.Server.RPCHost
}

func (cs *ClientSet) Client(shardKey string) (*sdk.ChainClient, error) {
	sc := cs.clients[shardKey]
	if sc == nil || sc.Client == nil {
		return nil, fmt.Errorf("client not found: %s", shardKey)
	}
	return sc.Client, nil
}

func (cs *ClientSet) Height(shardKey string) (uint64, error) {
	cc, err := cs.Client(shardKey)
	if err != nil {
		return 0, err
	}
	height, err := cc.GetCurrentBlockHeight()
	if err != nil {
		return 0, fmt.Errorf("get height %s: %w", shardKey, err)
	}
	return height, nil
}

func (cs *ClientSet) Close() {
	for _, sc := range cs.clients {
		if sc != nil && sc.Client != nil {
			_ = sc.Client.Stop()
		}
	}
}

type prepareShardStatus struct {
	Shard   Shard
	Label   string
	Height  uint64
	Missing []string
}

func collectPrepareStatuses(rt Runtime, clients *ClientSet) []prepareShardStatus {
	shards := allShards(rt.Config.BusinessShards)
	statuses := make([]prepareShardStatus, 0, len(shards))
	for _, shard := range shards {
		height, err := clients.Height(shard.Key)
		if err != nil {
			height = 0
		}
		statuses = append(statuses, prepareShardStatus{
			Shard:   shard,
			Label:   prepareShardLabel(shard),
			Height:  height,
			Missing: missingPrepareItems(rt, clients, shard),
		})
	}
	return statuses
}

func missingPrepareItems(rt Runtime, clients *ClientSet, shard Shard) []string {
	required := []string{workloadContract(rt), "poke_block", "sharding_heartbeat"}
	missing := make([]string, 0, len(required)+1)
	for _, contractName := range required {
		if !contractExists(clients, shard.Key, contractName) {
			missing = append(missing, contractName)
		}
	}
	return missing
}

func printPrepareStatuses(statuses []prepareShardStatus) {
	for _, status := range statuses {
		missing := "none"
		if len(status.Missing) > 0 {
			missing = strings.Join(status.Missing, ",")
		}
		fmt.Printf("  - %-10s %-12s height=%d missing=%s\n", status.Label, status.Shard.Key, status.Height, missing)
	}
}

func prepareShardLabel(shard Shard) string {
	if shard.Key == "bridge" {
		return "桥接分片"
	}
	if shard.Type == "business" || strings.HasPrefix(shard.Key, "business-") {
		id := shard.ID
		if id <= 0 {
			id = shardSortKey(shard.Key)
		}
		return fmt.Sprintf("业务分片 %d", id)
	}
	return shard.Label
}

func prepareReady(statuses []prepareShardStatus, targetHeight uint64) bool {
	for _, status := range statuses {
		if status.Height != targetHeight || len(status.Missing) > 0 {
			return false
		}
	}
	return true
}

func validatePrepareCanProceed(statuses []prepareShardStatus, targetHeight uint64) error {
	for _, status := range statuses {
		if status.Height > targetHeight {
			fmt.Printf("    ✗ %s(%s) 当前高度=%d，已经超过目标高度 %d，无法回退到演示起点\n",
				status.Label, status.Shard.Key, status.Height, targetHeight)
			return fmt.Errorf("准备模式无法继续：必须在高度 <%d 时完成合约部署和初始化。请停止节点并清理各分片 release 包下的 data/log 后重新启动，再运行 prepare", targetHeight)
		}
		if status.Height >= targetHeight && len(status.Missing) > 0 {
			fmt.Printf("    ✗ %s(%s) 当前高度=%d 已到分时边界，但仍缺少: %s\n",
				status.Label, status.Shard.Key, status.Height, strings.Join(status.Missing, ","))
			return fmt.Errorf("准备模式无法继续：必须在高度 <%d 时完成合约部署和初始化。请停止节点并清理各分片 release 包下的 data/log 后重新启动，再运行 prepare", targetHeight)
		}
	}
	return nil
}

func printCurrentPrepareHeights(rt Runtime, clients *ClientSet) {
	fmt.Printf("\n当前区块高度:\n")
	for shardID := 1; shardID <= rt.Config.BusinessShards; shardID++ {
		shardKey := fmt.Sprintf("business-%d", shardID)
		height, err := clients.Height(shardKey)
		if err != nil {
			fmt.Printf("  - 业务分片 %d: unknown (%v)\n", shardID, err)
			continue
		}
		fmt.Printf("  - 业务分片 %d: %d\n", shardID, height)
	}
	height, err := clients.Height("bridge")
	if err != nil {
		fmt.Printf("  - 桥接分片: unknown (%v)\n", err)
		return
	}
	fmt.Printf("  - 桥接分片: %d\n", height)
}

func contractExists(clients *ClientSet, shardKey, contractName string) bool {
	cc, err := clients.Client(shardKey)
	if err != nil {
		return false
	}
	_, err = cc.GetContractInfo(contractName)
	return err == nil
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func workloadContract(rt Runtime) string {
	name := strings.TrimSpace(rt.Config.WorkloadContract)
	if name == "" {
		return tokenContract
	}
	return name
}

func workloadContractPath(rt Runtime) string {
	switch workloadContract(rt) {
	case standardDFAContract:
		return rt.Config.StandardDFAContractPath
	case standardDFAExperimentContract:
		return rt.Config.StandardDFAExperimentPath
	default:
		return rt.Config.TokenBusinessContractPath
	}
}

func workloadUsesToken(rt Runtime) bool {
	return workloadContract(rt) == tokenContract
}

func isSupportedWorkloadContract(contract string) bool {
	switch strings.TrimSpace(contract) {
	case tokenContract, standardDFAContract, standardDFAExperimentContract:
		return true
	default:
		return false
	}
}

func workloadMethod(contract string) string {
	switch strings.TrimSpace(contract) {
	case standardDFAContract:
		return "Transfer"
	case standardDFAExperimentContract:
		return "TransferExperiment"
	default:
		return "transfer"
	}
}

func deployBridge(rt Runtime, clients *ClientSet) error {
	shard := Shard{Key: "bridge", Type: "bridge", ID: 0, Label: "bridge"}
	fmt.Println("\n>>> 先部署桥接分片基础合约")
	fmt.Println("    说明: bridge 部署同名业务合约；跨片执行时由系统从 globalstate 读取业务状态。")
	if err := deployContractIfMissing(rt, clients, shard, workloadContract(rt), workloadContractPath(rt)); err != nil {
		return err
	}
	if err := deployContractIfMissing(rt, clients, shard, "poke_block", rt.Config.PokeBlockContractPath); err != nil {
		return err
	}
	return deployContractIfMissing(rt, clients, shard, "sharding_heartbeat", rt.Config.HeartbeatContractPath)
}

func deployBusiness(rt Runtime, clients *ClientSet, shardID int) error {
	shard := Shard{Key: fmt.Sprintf("business-%d", shardID), Type: "business", ID: shardID}
	if err := deployContractIfMissing(rt, clients, shard, workloadContract(rt), workloadContractPath(rt)); err != nil {
		return err
	}
	if workloadUsesToken(rt) {
		if err := configBusinessShard(rt, clients, shard); err != nil {
			return err
		}
		if rt.Config.InitAccounts {
			fmt.Printf("✓ 使用默认余额账户模式，忽略 init_accounts=true | 分片: %s default_balance=%d\n",
				shard.Key, rt.Config.InitialBalance)
		} else {
			fmt.Printf("✓ 使用默认余额账户模式，跳过账户初始化 | 分片: %s default_balance=%d\n",
				shard.Key, rt.Config.InitialBalance)
		}
	} else if workloadContract(rt) == standardDFAExperimentContract {
		fmt.Printf("✓ 使用 DFA 实验延迟余额模式，跳过账户初始化 | 分片: %s initial_balance=%d\n",
			shard.Key, rt.Config.InitialBalance)
	} else if workloadContract(rt) == standardDFAContract {
		fmt.Printf("✓ 标准 DFA 初始供应量已在部署时铸造给合约管理员 | 分片: %s total_supply=%d\n",
			shard.Key, rt.Config.InitialBalance)
	}
	if err := deployContractIfMissing(rt, clients, shard, "poke_block", rt.Config.PokeBlockContractPath); err != nil {
		return err
	}
	if err := deployContractIfMissing(rt, clients, shard, "sharding_heartbeat", rt.Config.HeartbeatContractPath); err != nil {
		return err
	}
	return nil
}

func deployContractIfMissing(rt Runtime, clients *ClientSet, shard Shard, contractName, contractPath string) error {
	cc, err := clients.Client(shard.Key)
	if err != nil {
		return err
	}
	fmt.Printf("\n>>> 部署合约 [%s] 到 [%s]...\n", contractName, shard.Key)
	if _, err := cc.GetContractInfo(contractName); err == nil {
		fmt.Printf("✓ 合约 [%s] 已存在于 [%s]，跳过部署\n", contractName, shard.Key)
		return nil
	}
	if err := checkDockerGoBinary(contractPath); err != nil {
		return err
	}
	admins, err := shardAdmins(rt, shard)
	if err != nil {
		return err
	}
	payload, err := cc.CreateContractCreatePayload(contractName, defaultVersion, contractPath,
		pbcommon.RuntimeType_DOCKER_GO, contractInitArgs(rt, contractName))
	if err != nil {
		return fmt.Errorf("create deploy payload %s/%s: %w", shard.Key, contractName, err)
	}
	endorsers := make([]*pbcommon.EndorsementEntry, 0, len(admins))
	for _, admin := range admins {
		entry, err := sdkutils.MakeEndorserWithPath(admin.SignKeyPath, admin.SignCrtPath, payload)
		if err != nil {
			return fmt.Errorf("endorse %s/%s admin=%s: %w", shard.Key, contractName, admin.Name, err)
		}
		endorsers = append(endorsers, entry)
	}
	fmt.Printf("开始部署合约 [%s]...\n", contractName)
	resp, err := cc.SendContractManageRequest(payload, endorsers, rt.Config.DeployTimeoutSeconds, true)
	if err != nil {
		if isSyncTimeout(resp, err) && waitContractExists(cc, contractName, time.Duration(rt.Config.DeployConfirmWaitSeconds)*time.Second) {
			fmt.Printf("    ⚠ 合约 [%s] 部署交易同步等待超时，继续轮询链上合约状态 | 分片=%s wait=%ds\n",
				contractName, shard.Key, rt.Config.DeployConfirmWaitSeconds)
			fmt.Printf("✓ 合约 [%s] 部署成功\n", contractName)
			return nil
		}
		return fmt.Errorf("deploy %s/%s: %w", shard.Key, contractName, err)
	}
	if isSyncTimeout(resp, nil) {
		fmt.Printf("    ⚠ 合约 [%s] 部署交易同步等待超时，继续轮询链上合约状态 | 分片=%s wait=%ds\n",
			contractName, shard.Key, rt.Config.DeployConfirmWaitSeconds)
		if waitContractExists(cc, contractName, time.Duration(rt.Config.DeployConfirmWaitSeconds)*time.Second) {
			fmt.Printf("✓ 合约 [%s] 部署成功\n", contractName)
			return nil
		}
	}
	if err := checkTxResponse(resp); err != nil {
		return fmt.Errorf("deploy %s/%s: %w", shard.Key, contractName, err)
	}
	fmt.Printf("✓ 合约 [%s] 部署成功\n", contractName)
	return nil
}

func contractInitArgs(rt Runtime, contractName string) []*pbcommon.KeyValuePair {
	balance := strconv.FormatInt(rt.Config.InitialBalance, 10)
	switch strings.TrimSpace(contractName) {
	case standardDFAContract:
		return []*pbcommon.KeyValuePair{
			{Key: "name", Value: []byte("ChainMaker Standard DFA")},
			{Key: "symbol", Value: []byte("DFA")},
			{Key: "decimals", Value: []byte("18")},
			{Key: "totalSupply", Value: []byte(balance)},
		}
	case standardDFAExperimentContract:
		return []*pbcommon.KeyValuePair{
			{Key: "name", Value: []byte("ChainMaker DFA Experiment")},
			{Key: "symbol", Value: []byte("DFAE")},
			{Key: "decimals", Value: []byte("18")},
			{Key: "experimentInitialBalance", Value: []byte(balance)},
		}
	default:
		return nil
	}
}

func configBusinessShard(rt Runtime, clients *ClientSet, shard Shard) error {
	args := []*pbcommon.KeyValuePair{
		{Key: "shard_index", Value: []byte(strconv.Itoa(shard.ID - 1))},
		{Key: "shard_number", Value: []byte(strconv.Itoa(rt.Config.BusinessShards))},
	}
	fmt.Printf(">>> 自动配置分片参数 | 分片: %-12s | 合约: %-14s | shard_index=%d shard_number=%d\n",
		shard.Key, "token_business", shard.ID-1, rt.Config.BusinessShards)
	_, err := invokeContract(rt, clients, shard.Key, "token_business", "config_shard", args, true)
	return err
}

func advanceAllShards(rt Runtime, clients *ClientSet, target uint64) error {
	jobs := make([]namedJob, 0, rt.Config.BusinessShards+1)
	for _, shard := range allShards(rt.Config.BusinessShards) {
		shard := shard
		jobs = append(jobs, namedJob{
			name: shard.Key,
			run: func() error {
				return advanceShard(rt, clients, shard.Key, target)
			},
		})
	}
	return runJobs("并行推进所有分片高度", jobs, rt.Config.PrepareConcurrency)
}

func advanceShard(rt Runtime, clients *ClientSet, shardKey string, target uint64) error {
	for {
		height, err := clients.Height(shardKey)
		if err != nil {
			return err
		}
		if height >= target {
			return nil
		}
		args := []*pbcommon.KeyValuePair{{Key: "tag", Value: []byte(fmt.Sprintf("prepare_%s_%d", sanitizeFileName(shardKey), height+1))}}
		if _, err := invokeContract(rt, clients, shardKey, "poke_block", "poke", args, true); err != nil {
			return err
		}
		deadline := time.Now().Add(time.Duration(rt.Config.HeightWaitSeconds) * time.Second)
		for time.Now().Before(deadline) {
			newHeight, _ := clients.Height(shardKey)
			if newHeight > height {
				break
			}
			time.Sleep(time.Second)
		}
	}
}

func invokeContract(rt Runtime, clients *ClientSet, shardKey, contract, method string, args []*pbcommon.KeyValuePair, syncResult bool) (*pbcommon.TxResponse, error) {
	cc, err := clients.Client(shardKey)
	if err != nil {
		return nil, err
	}
	kvs := make([]*pbcommon.KeyValuePair, 0, len(args)+1)
	kvs = append(kvs, &pbcommon.KeyValuePair{Key: "method", Value: []byte(method)})
	kvs = append(kvs, args...)
	resp, err := cc.InvokeContract(contract, "invoke_contract", "", kvs, rt.Config.TxTimeoutSeconds, syncResult)
	if err != nil {
		if resp != nil && strings.TrimSpace(resp.TxId) != "" && isSyncTimeout(resp, err) {
			return resp, nil
		}
		return resp, err
	}
	if resp == nil {
		return nil, fmt.Errorf("nil tx response")
	}
	if resp.Code != pbcommon.TxStatusCode_SUCCESS {
		if resp.Code == pbcommon.TxStatusCode_TIMEOUT && strings.Contains(resp.Message, "当前未轮到") && strings.TrimSpace(resp.TxId) != "" {
			return resp, nil
		}
		return resp, fmt.Errorf("tx failed: code=%s msg=%s", resp.Code.String(), resp.Message)
	}
	if syncResult && resp.ContractResult != nil && resp.ContractResult.Code != 0 {
		return resp, fmt.Errorf("contract failed: code=%d msg=%s", resp.ContractResult.Code, resp.ContractResult.Message)
	}
	return resp, nil
}

func sendWorkload(rt Runtime, clients *ClientSet, txs []WorkloadTx, rate int, workerCount int) ([]SubmitResult, map[string]int) {
	if workerCount > len(txs) {
		workerCount = len(txs)
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	results := make([]SubmitResult, len(txs))
	failures := make(map[string]int)
	var failuresMu sync.Mutex
	var started int64
	var completed int64
	var submitted int64
	var failed int64

	prepareJobs := make(chan int, workerCount*2)
	prepared := make(chan preparedSubmit, workerCount*2)
	builderCount := minInt(512, maxInt(32, workerCount/8))
	var builders sync.WaitGroup
	for i := 0; i < builderCount; i++ {
		builders.Add(1)
		go func() {
			defer builders.Done()
			for idx := range prepareJobs {
				prepared <- prepareSubmit(rt, clients, idx, txs[idx])
			}
		}()
	}
	go func() {
		builders.Wait()
		close(prepared)
	}()

	gate := newSendRateGate(rate)
	var senders sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for item := range prepared {
				result := sendPreparedSubmit(rt, item, gate, &started)
				results[item.Index] = result
				if result.TxID != "" && result.Error == "" {
					atomic.AddInt64(&submitted, 1)
				} else {
					atomic.AddInt64(&failed, 1)
					reason := result.Error
					if reason == "" {
						reason = "unknown"
					}
					failuresMu.Lock()
					failures[reason]++
					failuresMu.Unlock()
				}
				atomic.AddInt64(&completed, 1)
			}
		}()
	}

	reportDone := make(chan struct{})
	go reportSendProgress(len(txs), &started, &completed, &submitted, &failed, reportDone)
	for idx := range txs {
		prepareJobs <- idx
	}
	close(prepareJobs)
	senders.Wait()
	close(reportDone)
	fmt.Printf("  send progress: started=%d/%d completed=%d submitted=%d failed=%d\n",
		atomic.LoadInt64(&started), len(txs), atomic.LoadInt64(&completed),
		atomic.LoadInt64(&submitted), atomic.LoadInt64(&failed))
	return results, failures
}

type preparedSubmit struct {
	Index       int
	Tx          WorkloadTx
	TargetShard string
	Client      *sdk.ChainClient
	Request     *pbcommon.TxRequest
	Error       error
}

func prepareSubmit(rt Runtime, clients *ClientSet, idx int, tx WorkloadTx) preparedSubmit {
	target := txTargetShard(tx)
	item := preparedSubmit{Index: idx, Tx: tx, TargetShard: target}
	cc, err := clients.Client(target)
	if err != nil {
		item.Error = err
		return item
	}
	args := txInvokeArgs(rt, tx)
	kvs := make([]*pbcommon.KeyValuePair, 0, len(args)+1)
	kvs = append(kvs, &pbcommon.KeyValuePair{Key: "method", Value: []byte(tx.Method)})
	kvs = append(kvs, args...)
	item.Client = cc
	item.Request, item.Error = cc.GetTxRequest(tx.Contract, "invoke_contract", "", kvs)
	return item
}

func sendPreparedSubmit(rt Runtime, item preparedSubmit, gate *sendRateGate, started *int64) SubmitResult {
	result := SubmitResult{Seq: item.Tx.Seq, TargetShard: item.TargetShard, TxType: item.Tx.TxType}
	if item.Error != nil {
		result.Error = item.Error.Error()
		return result
	}
	gate.Wait()
	result.StartedAt = time.Now()
	atomic.AddInt64(started, 1)
	resp, err := item.Client.SendTxRequest(item.Request, rt.Config.TxTimeoutSeconds, false)
	result.SubmittedAt = time.Now()
	if err != nil {
		if resp != nil {
			result.TxID = strings.TrimSpace(resp.TxId)
		}
		if result.TxID == "" || !isSyncTimeout(resp, err) {
			result.Error = err.Error()
			return result
		}
	}
	if resp == nil {
		result.Error = "nil response"
		return result
	}
	result.TxID = strings.TrimSpace(resp.TxId)
	if resp.Code != pbcommon.TxStatusCode_SUCCESS {
		acceptedWhilePaused := resp.Code == pbcommon.TxStatusCode_TIMEOUT &&
			strings.Contains(resp.Message, "当前未轮到") && result.TxID != ""
		if !acceptedWhilePaused {
			result.Error = fmt.Sprintf("tx failed: code=%s msg=%s", resp.Code.String(), resp.Message)
		}
	} else if result.TxID == "" {
		result.Error = "empty tx id"
	}
	return result
}

type sendRateGate struct {
	rate    int64
	once    sync.Once
	start   time.Time
	ordinal int64
}

func newSendRateGate(rate int) *sendRateGate {
	if rate <= 0 {
		rate = 1
	}
	return &sendRateGate{rate: int64(rate)}
}

func (g *sendRateGate) Wait() {
	g.once.Do(func() { g.start = time.Now() })
	ordinal := atomic.AddInt64(&g.ordinal, 1) - 1
	target := g.start.Add(time.Duration(ordinal * int64(time.Second) / g.rate))
	if delay := time.Until(target); delay > 0 {
		time.Sleep(delay)
	}
}

func reportSendProgress(total int, started, completed, submitted, failed *int64, done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	previousStarted := int64(0)
	previousCompleted := int64(0)
	previousSubmitted := int64(0)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			currentStarted := atomic.LoadInt64(started)
			currentCompleted := atomic.LoadInt64(completed)
			currentSubmitted := atomic.LoadInt64(submitted)
			fmt.Printf("  send progress: started=%d/%d completed=%d submitted=%d failed=%d offered_rate=%d accepted_rate=%d response_rate=%d in_flight=%d\n",
				currentStarted, total, currentCompleted, currentSubmitted, atomic.LoadInt64(failed),
				currentStarted-previousStarted, currentSubmitted-previousSubmitted,
				currentCompleted-previousCompleted, currentStarted-currentCompleted)
			previousStarted = currentStarted
			previousCompleted = currentCompleted
			previousSubmitted = currentSubmitted
		}
	}
}

func txInvokeArgs(rt Runtime, tx WorkloadTx) []*pbcommon.KeyValuePair {
	amount, _ := tx.Amount.Int64()
	if amount <= 0 {
		amount = rt.Config.Amount
	}
	args := []*pbcommon.KeyValuePair{
		{Key: "from", Value: []byte(tx.From)},
		{Key: "to", Value: []byte(tx.To)},
		{Key: "amount", Value: []byte(strconv.FormatInt(amount, 10))},
	}
	if tx.TxType == "cross" {
		args = append(args,
			&pbcommon.KeyValuePair{Key: "from_shard", Value: []byte(tx.FromShard)},
			&pbcommon.KeyValuePair{Key: "to_shard", Value: []byte(tx.ToShard)},
			&pbcommon.KeyValuePair{Key: "__cross_shard", Value: []byte("true")},
		)
	}
	return args
}

func loadWorkload(path string, businessShards int, defaultContract string) ([]WorkloadTx, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	var txs []WorkloadTx
	if trimmed[0] == '[' {
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		if err := dec.Decode(&txs); err != nil {
			return nil, err
		}
	} else {
		scanner := bufio.NewScanner(bytes.NewReader(trimmed))
		scanner.Buffer(make([]byte, 1024), 16*1024*1024)
		lineNo := 0
		for scanner.Scan() {
			lineNo++
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			var tx WorkloadTx
			if err := dec.Decode(&tx); err != nil {
				return nil, fmt.Errorf("line %d: %w", lineNo, err)
			}
			txs = append(txs, tx)
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	for i := range txs {
		if err := normalizeTx(&txs[i], businessShards, defaultContract); err != nil {
			return nil, fmt.Errorf("tx %d: %w", i+1, err)
		}
	}
	return txs, nil
}

func normalizeTx(tx *WorkloadTx, businessShards int, defaultContract string) error {
	tx.Contract = strings.TrimSpace(tx.Contract)
	tx.Method = strings.TrimSpace(tx.Method)
	if tx.Contract == "" {
		tx.Contract = strings.TrimSpace(defaultContract)
		if tx.Contract == "" {
			tx.Contract = tokenContract
		}
	}
	if tx.Method == "" {
		tx.Method = workloadMethod(tx.Contract)
	}
	tx.From = strings.TrimSpace(tx.From)
	tx.To = strings.TrimSpace(tx.To)
	if tx.From == "" || tx.To == "" {
		return fmt.Errorf("from/to is empty")
	}
	if !isSupportedWorkloadContract(tx.Contract) {
		return fmt.Errorf("unsupported workload contract: %s", tx.Contract)
	}
	if strings.TrimSpace(defaultContract) != "" && tx.Contract != strings.TrimSpace(defaultContract) {
		return fmt.Errorf("dataset contract %s does not match configured workload_contract %s", tx.Contract, defaultContract)
	}
	if tx.Method != workloadMethod(tx.Contract) {
		return fmt.Errorf("contract %s requires workload method %s, got %s",
			tx.Contract, workloadMethod(tx.Contract), tx.Method)
	}
	fromShard := parseShard(tx.FromShard, businessShards)
	toShard := parseShard(tx.ToShard, businessShards)
	if fromShard == "" {
		fromShard = fmt.Sprintf("business-%d", computeBusinessShardIndex(tx.From, businessShards)+1)
	}
	if toShard == "" {
		toShard = fmt.Sprintf("business-%d", computeBusinessShardIndex(tx.To, businessShards)+1)
	}
	tx.FromShard = fromShard
	tx.ToShard = toShard
	if fromShard == toShard {
		tx.TxType = "intra"
	} else {
		tx.TxType = "cross"
	}
	return nil
}

func txTargetShard(tx WorkloadTx) string {
	if tx.TxType == "cross" {
		return "bridge"
	}
	return tx.FromShard
}

func workloadTargetShards(txs []WorkloadTx) []string {
	seen := make(map[string]struct{})
	for _, tx := range txs {
		target := txTargetShard(tx)
		if target != "" {
			seen[target] = struct{}{}
		}
	}
	shards := make([]string, 0, len(seen))
	for shard := range seen {
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool {
		return shardSortKey(shards[i]) < shardSortKey(shards[j])
	})
	return shards
}

func captureHeights(clients *ClientSet, shards []string) (map[string]uint64, error) {
	heights := make(map[string]uint64, len(shards))
	for _, shard := range shards {
		height, err := clients.Height(shard)
		if err != nil {
			return nil, fmt.Errorf("capture height %s: %w", shard, err)
		}
		heights[shard] = height
	}
	return heights, nil
}

func countSubmitted(results []SubmitResult) int64 {
	var count int64
	for _, result := range results {
		if result.TxID != "" && result.Error == "" {
			count++
		}
	}
	return count
}

func countSubmitFailed(results []SubmitResult) int64 {
	var count int64
	for _, result := range results {
		if result.TxID == "" || result.Error != "" {
			count++
		}
	}
	return count
}

func collectRunConfirmations(rt Runtime, clients *ClientSet, targetShards []string, startHeights map[string]uint64, results []SubmitResult) ([]BlockRow, int) {
	submitted := make(map[string]int)
	for i, result := range results {
		if result.TxID != "" && result.Error == "" {
			submitted[result.TxID] = i
		}
	}
	if len(submitted) == 0 {
		return nil, 0
	}
	nextHeight := make(map[string]uint64, len(targetShards))
	for _, shard := range targetShards {
		nextHeight[shard] = startHeights[shard] + 1
	}
	blockTimings := loadBlockProductionTimings(rt, targetShards)
	timeout := time.Duration(rt.Config.ConfirmWaitSeconds) * time.Second
	if timeout <= 0 {
		timeout = 600 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var rows []BlockRow
	confirmed := 0
	lastLog := time.Now()
	currentHeights := make(map[string]uint64, len(targetShards))
	for {
		for _, shard := range targetShards {
			height, err := clients.Height(shard)
			if err != nil {
				fmt.Printf("  warn: get height %s: %v\n", shard, err)
				continue
			}
			currentHeights[shard] = height
			next := nextHeight[shard]
			if height < next {
				continue
			}
			cc, err := clients.Client(shard)
			if err != nil {
				fmt.Printf("  warn: get client %s: %v\n", shard, err)
				continue
			}
			for h := next; h <= height; h++ {
				blockInfo, err := cc.GetBlockByHeight(h, false)
				if err != nil {
					fmt.Printf("  warn: get block %s/%d: %v\n", shard, h, err)
					break
				}
				timing, hasTiming := blockTimings[blockTimingKey{Shard: shard, Height: h}]
				row, matched := matchSubmittedBlock(shard, h, blockInfo, submitted, results, timing, hasTiming)
				if row.TotalTxCount > 0 {
					rows = append(rows, row)
				}
				if matched > 0 {
					confirmed += matched
				}
				nextHeight[shard] = h + 1
			}
		}
		if confirmed >= len(submitted) || time.Now().After(deadline) {
			break
		}
		if time.Since(lastLog) >= 5*time.Second {
			fmt.Printf("  confirmations: confirmed=%d/%d heights=%s next_scan=%s\n",
				confirmed, len(submitted),
				formatShardHeights(targetShards, currentHeights, 8),
				formatShardHeights(targetShards, nextHeight, 8))
			lastLog = time.Now()
		}
		time.Sleep(2 * time.Second)
	}
	blockTimings = loadBlockProductionTimings(rt, targetShards)
	applyBlockTimings(rows, blockTimings)
	fillPhaseSync(rt, rows)
	applyResultConfirmationTimings(results, blockTimings)
	sortBlockRows(rows)
	fillBlockRowCumulative(rows)
	return rows, confirmed
}

func applyLocalScheduleMetricsForRun(rt Runtime, rows []BlockRow, results []SubmitResult, started, finished time.Time, shards []string) int {
	events := loadScheduleMetricEvents(rt, shards)
	if len(events) == 0 {
		return 0
	}
	since := started.Add(-10 * time.Minute)
	until := finished.Add(10 * time.Minute)
	filtered := make([]scheduleMetricEvent, 0, len(events))
	for _, event := range events {
		if !eventInWindow(event.At, since, until) {
			continue
		}
		event.Server = rt.Server.Name
		event.Host = rt.Server.Host
		event.NodeID = rt.Server.NodeID
		filtered = append(filtered, event)
	}
	if len(filtered) == 0 {
		return 0
	}
	timings := timingsFromScheduleMetricEvents(filtered)
	applyBlockTimings(rows, timings)
	if !applyPhaseSyncFromMetricEvents(rt, rows, blockRowShards(rows), filtered) {
		fillPhaseSyncByBlockOrder(rt, rows)
	}
	applyResultConfirmationTimings(results, timings)
	sortBlockRows(rows)
	fillBlockRowCumulative(rows)
	return len(filtered)
}

func matchSubmittedBlock(shard string, height uint64, blockInfo *pbcommon.BlockInfo, submitted map[string]int, results []SubmitResult, timing blockTiming, hasTiming bool) (BlockRow, int) {
	if blockInfo == nil || blockInfo.Block == nil || blockInfo.Block.Header == nil {
		return BlockRow{}, 0
	}
	header := blockInfo.Block.Header
	totalTx := uint64(header.TxCount)
	if totalTx == 0 && len(blockInfo.Block.Txs) > 0 {
		totalTx = uint64(len(blockInfo.Block.Txs))
	}
	var userTx uint64
	var crossTx uint64
	var intraTx uint64
	blockTime := timestampToTime(header.BlockTimestamp)
	startTimestampMs := int64(0)
	startSource := ""
	consensusStartMs := int64(0)
	commitTimestampMs := int64(0)
	intervalCommitMs := int64(0)
	blockTimeMillis := int64(-1)
	blockTimeSource := ""
	specialBlock := false
	if hasTiming {
		if !timing.AllowAt.IsZero() {
			startTimestampMs = timing.AllowAt.UnixMilli()
		}
		startSource = timing.StartSource
		specialBlock = timing.Special
		if !timing.ConsensusStartAt.IsZero() {
			consensusStartMs = timing.ConsensusStartAt.UnixMilli()
		}
		if !timing.CommitAt.IsZero() {
			blockTime = timing.CommitAt
			commitTimestampMs = timing.CommitAt.UnixMilli()
		}
		if !timing.IntervalCommitAt.IsZero() {
			intervalCommitMs = timing.IntervalCommitAt.UnixMilli()
		}
		blockTimeMillis = timing.DurationMillis
		blockTimeSource = blockTimingSource(timing)
	}
	for _, tx := range blockInfo.Block.Txs {
		if tx == nil || tx.Payload == nil {
			continue
		}
		idx, ok := submitted[strings.TrimSpace(tx.Payload.TxId)]
		if !ok || results[idx].Confirmed {
			continue
		}
		results[idx].Confirmed = true
		results[idx].ConfirmedAt = blockTime
		if !results[idx].StartedAt.IsZero() {
			latency := blockTime.Sub(results[idx].StartedAt).Milliseconds()
			if latency < 0 {
				latency = 0
			}
			results[idx].ConfirmLatencyMs = latency
		}
		results[idx].BlockShard = shard
		results[idx].BlockHeight = height
		results[idx].BlockTimestamp = header.BlockTimestamp
		results[idx].BlockHash = fmt.Sprintf("%x", header.BlockHash)
		userTx++
		if results[idx].TxType == "cross" {
			crossTx++
		} else {
			intraTx++
		}
	}
	if totalTx == 0 {
		return BlockRow{}, 0
	}
	internalTx := uint64(0)
	if totalTx >= userTx {
		internalTx = totalTx - userTx
	}
	return BlockRow{
		Shard:             shard,
		Height:            height,
		BlockHash:         fmt.Sprintf("%x", header.BlockHash),
		Timestamp:         header.BlockTimestamp,
		StartTimestampMs:  startTimestampMs,
		StartSource:       startSource,
		SpecialBlock:      specialBlock,
		SchedulerCommit:   isSchedulerCommitSource(timing.CommitSource),
		ConsensusStartMs:  consensusStartMs,
		CommitTimestampMs: commitTimestampMs,
		BlockTimeMillis:   blockTimeMillis,
		BlockTimeSource:   blockTimeSource,
		IntervalCommitMs:  intervalCommitMs,
		BlockIntervalMs:   -1,
		PhaseSyncMillis:   -1,
		TotalTxCount:      totalTx,
		UserTxCount:       userTx,
		CrossUserTxCount:  crossTx,
		IntraUserTxCount:  intraTx,
		InternalTxCount:   internalTx,
	}, int(userTx)
}

func applyBlockTimings(rows []BlockRow, timings map[blockTimingKey]blockTiming) {
	for i := range rows {
		timing, ok := timings[blockTimingKey{Shard: rows[i].Shard, Height: rows[i].Height}]
		if !ok {
			continue
		}
		if !timing.AllowAt.IsZero() {
			rows[i].StartTimestampMs = timing.AllowAt.UnixMilli()
		}
		rows[i].StartSource = timing.StartSource
		rows[i].SpecialBlock = timing.Special
		if !timing.ConsensusStartAt.IsZero() {
			rows[i].ConsensusStartMs = timing.ConsensusStartAt.UnixMilli()
		}
		if !timing.CommitAt.IsZero() {
			rows[i].CommitTimestampMs = timing.CommitAt.UnixMilli()
		}
		if !timing.IntervalCommitAt.IsZero() {
			rows[i].IntervalCommitMs = timing.IntervalCommitAt.UnixMilli()
		}
		rows[i].SchedulerCommit = isSchedulerCommitSource(timing.CommitSource)
		rows[i].BlockTimeMillis = timing.DurationMillis
		rows[i].BlockTimeSource = blockTimingSource(timing)
	}
}

func blockTimingSource(timing blockTiming) string {
	if timing.DurationMillis >= 0 &&
		timing.StartSource == blockMetadataStartSource &&
		timing.CommitSource == localCommitCompleteSource {
		return timing.StartSource + "->" + timing.CommitSource
	}
	if timing.DurationMillis >= 0 && timing.StartSource == "sched_metric_allow" && timing.CommitSource == "sched_metric" {
		return timing.StartSource + "->" + timing.CommitSource
	}
	return "missing_metric"
}

func finalizeBlockTimingFields(rows []BlockRow) {
	for i := range rows {
		validSource := rows[i].BlockTimeSource ==
			blockMetadataStartSource+"->"+localCommitCompleteSource ||
			rows[i].BlockTimeSource == "sched_metric_allow->sched_metric"
		if validSource &&
			rows[i].CommitTimestampMs > 0 &&
			rows[i].StartTimestampMs > 0 &&
			rows[i].BlockTimeMillis >= 0 {
			continue
		}
		rows[i].BlockTimeMillis = -1
		rows[i].BlockTimeSource = "missing_metric"
	}
}

func applyResultConfirmationTimings(results []SubmitResult, timings map[blockTimingKey]blockTiming) {
	for i := range results {
		if !results[i].Confirmed || results[i].BlockShard == "" || results[i].BlockHeight == 0 {
			continue
		}
		timing, ok := timings[blockTimingKey{Shard: results[i].BlockShard, Height: results[i].BlockHeight}]
		if !ok || timing.CommitAt.IsZero() {
			continue
		}
		results[i].ConfirmedAt = timing.CommitAt
		if !results[i].StartedAt.IsZero() {
			latency := timing.CommitAt.Sub(results[i].StartedAt).Milliseconds()
			if latency < 0 {
				latency = 0
			}
			results[i].ConfirmLatencyMs = latency
		}
	}
}

func sortBlockRows(rows []BlockRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Timestamp != rows[j].Timestamp {
			return timestampBefore(rows[i].Timestamp, rows[j].Timestamp)
		}
		if rows[i].Shard != rows[j].Shard {
			return shardSortKey(rows[i].Shard) < shardSortKey(rows[j].Shard)
		}
		return rows[i].Height < rows[j].Height
	})
}

func fillBlockRowCumulative(rows []BlockRow) {
	if len(rows) == 0 {
		return
	}
	firstTs := rows[0].Timestamp
	var cumulativeTotal uint64
	var cumulativeUser uint64
	for i := range rows {
		cumulativeTotal += rows[i].TotalTxCount
		cumulativeUser += rows[i].UserTxCount
		rows[i].CumulativeTotalTx = cumulativeTotal
		rows[i].CumulativeUserTx = cumulativeUser
		rows[i].ElapsedMillis = timestampDiffMillis(firstTs, rows[i].Timestamp)
	}
}

func fillBlockRowIntervals(rows []BlockRow) {
	if len(rows) == 0 {
		return
	}
	byShard := make(map[string][]int)
	for i := range rows {
		rows[i].BlockIntervalMs = -1
		rows[i].BlockIntervalSrc = "missing_commit_time"
		byShard[rows[i].Shard] = append(byShard[rows[i].Shard], i)
	}
	for _, indexes := range byShard {
		sort.SliceStable(indexes, func(i, j int) bool {
			left := rows[indexes[i]]
			right := rows[indexes[j]]
			if left.Height != right.Height {
				return left.Height < right.Height
			}
			return timestampBefore(left.Timestamp, right.Timestamp)
		})
		var prevCommitMs int64
		prevSource := ""
		for _, rowIndex := range indexes {
			commitMs, source := blockRowIntervalCommitTime(rows[rowIndex])
			if commitMs <= 0 {
				rows[rowIndex].BlockIntervalMs = -1
				rows[rowIndex].BlockIntervalSrc = "missing_commit_time"
				continue
			}
			if prevCommitMs <= 0 {
				rows[rowIndex].BlockIntervalMs = 0
				rows[rowIndex].BlockIntervalSrc = source
			} else {
				intervalMs := commitMs - prevCommitMs
				if intervalMs < 0 {
					intervalMs = 0
				}
				rows[rowIndex].BlockIntervalMs = intervalMs
				if source == prevSource {
					rows[rowIndex].BlockIntervalSrc = source
				} else {
					rows[rowIndex].BlockIntervalSrc = source + "_mixed_previous"
				}
			}
			prevCommitMs = commitMs
			prevSource = source
		}
	}
}

func blockRowIntervalCommitTime(row BlockRow) (int64, string) {
	if row.IntervalCommitMs > 0 {
		return row.IntervalCommitMs, "commit_log"
	}
	if row.CommitTimestampMs > 0 {
		return row.CommitTimestampMs, "sched_metric_commit"
	}
	return 0, ""
}

func fillPhaseSync(rt Runtime, rows []BlockRow) {
	for i := range rows {
		rows[i].PhaseSyncMillis = -1
	}
	shards := blockRowShards(rows)
	if fillPhaseSyncFromMetricEvents(rt, rows, shards) {
		return
	}
	fillPhaseSyncByBlockOrder(rt, rows)
}

func fillPhaseSyncFromMetricEvents(rt Runtime, rows []BlockRow, shards []string) bool {
	events := loadScheduleMetricEvents(rt, shards)
	return applyPhaseSyncFromMetricEvents(rt, rows, shards, events)
}

func applyPhaseSyncFromMetricEvents(rt Runtime, rows []BlockRow, shards []string, events []scheduleMetricEvent) bool {
	if len(events) == 0 {
		return false
	}
	rowByBlock := make(map[blockTimingKey]int, len(rows))
	for i := range rows {
		rowByBlock[blockTimingKey{Shard: rows[i].Shard, Height: rows[i].Height}] = i
	}

	bridgeEnds := make([]scheduleMetricEvent, 0)
	businessEndsByShard := make(map[string][]scheduleMetricEvent)
	allowsByShard := make(map[string][]scheduleMetricEvent)
	for _, event := range events {
		switch event.Event {
		case "phase_end":
			completed := strings.ToUpper(event.Fields["completed_phase"])
			if event.Shard == "bridge" || completed == "BRIDGE" {
				bridgeEnds = append(bridgeEnds, event)
			} else if strings.HasPrefix(event.Shard, "business-") || completed == "BUSINESS" {
				businessEndsByShard[event.Shard] = append(businessEndsByShard[event.Shard], event)
			}
		case "allow_produce":
			allowsByShard[event.Shard] = append(allowsByShard[event.Shard], event)
		}
	}
	if len(bridgeEnds) == 0 && len(businessEndsByShard) == 0 {
		return false
	}

	sortScheduleMetricEvents(bridgeEnds)
	for shard := range businessEndsByShard {
		sortScheduleMetricEvents(businessEndsByShard[shard])
	}
	for shard := range allowsByShard {
		sortScheduleMetricEvents(allowsByShard[shard])
	}

	filled := 0
	for shard, allows := range allowsByShard {
		if !strings.HasPrefix(shard, "business-") {
			continue
		}
		var usedBridgeEnd time.Time
		for _, allow := range allows {
			height, ok := metricUint(allow.Fields, "height")
			if !ok {
				continue
			}
			bridgeEnd, ok := latestEventBefore(bridgeEnds, allow.At)
			if !ok || bridgeEnd.At.Equal(usedBridgeEnd) {
				continue
			}
			rowIndex, ok := rowByBlock[blockTimingKey{Shard: shard, Height: height}]
			if !ok {
				continue
			}
			rows[rowIndex].PhaseSyncMillis = nonNegativeMillis(allow.At.Sub(bridgeEnd.At).Milliseconds())
			usedBridgeEnd = bridgeEnd.At
			filled++
		}
	}

	businessShards := make([]string, 0, rt.Config.BusinessShards)
	for i := 1; i <= rt.Config.BusinessShards; i++ {
		businessShards = append(businessShards, fmt.Sprintf("business-%d", i))
	}
	bridgeAllows := allowsByShard["bridge"]
	usedBusinessPhase := make(map[string]struct{})
	for _, allow := range bridgeAllows {
		height, ok := metricUint(allow.Fields, "height")
		if !ok {
			continue
		}
		phaseKey, businessEndAt, ok := latestBusinessPhaseEndBefore(businessEndsByShard, businessShards, allow.At)
		if !ok {
			continue
		}
		if _, exists := usedBusinessPhase[phaseKey]; exists {
			continue
		}
		rowIndex, ok := rowByBlock[blockTimingKey{Shard: "bridge", Height: height}]
		if !ok {
			continue
		}
		rows[rowIndex].PhaseSyncMillis = nonNegativeMillis(allow.At.Sub(businessEndAt).Milliseconds())
		usedBusinessPhase[phaseKey] = struct{}{}
		filled++
	}
	return filled > 0
}

func fillPhaseSyncByBlockOrder(rt Runtime, rows []BlockRow) {

	businessBatch := rt.Config.IntraConsensusRounds
	if businessBatch <= 0 {
		businessBatch = 5
	}
	bridgeBatch := rt.Config.InterConsensusRounds
	if bridgeBatch <= 0 {
		bridgeBatch = 8
	}

	byShard := make(map[string][]int)
	bridgeRows := make([]int, 0)
	for i := range rows {
		if !isMeasuredSchedulerBlock(rows[i]) {
			continue
		}
		switch performancePhase(rows[i].Shard) {
		case "business":
			byShard[rows[i].Shard] = append(byShard[rows[i].Shard], i)
		case "bridge":
			bridgeRows = append(bridgeRows, i)
		}
	}
	for shard := range byShard {
		sort.SliceStable(byShard[shard], func(i, j int) bool {
			return rows[byShard[shard][i]].Height < rows[byShard[shard][j]].Height
		})
	}
	sort.SliceStable(bridgeRows, func(i, j int) bool {
		return rows[bridgeRows[i]].Height < rows[bridgeRows[j]].Height
	})

	businessPhaseLastCommit := make(map[int]int64)
	for _, indices := range byShard {
		for pos, rowIndex := range indices {
			phaseIndex := pos / businessBatch
			posInPhase := pos % businessBatch
			if posInPhase == businessBatch-1 && rows[rowIndex].CommitTimestampMs > businessPhaseLastCommit[phaseIndex] {
				businessPhaseLastCommit[phaseIndex] = rows[rowIndex].CommitTimestampMs
			}
		}
	}

	bridgePhaseLastCommit := make(map[int]int64)
	for pos, rowIndex := range bridgeRows {
		phaseIndex := pos / bridgeBatch
		posInPhase := pos % bridgeBatch
		if posInPhase == bridgeBatch-1 {
			bridgePhaseLastCommit[phaseIndex] = rows[rowIndex].CommitTimestampMs
		}
	}

	for _, rowIndex := range bridgeRows {
		pos := schedulerBlockPosition(rows, bridgeRows, rowIndex)
		if pos < 0 || pos%bridgeBatch != 0 {
			continue
		}
		phaseIndex := pos / bridgeBatch
		prevBusinessCommit := businessPhaseLastCommit[phaseIndex]
		if prevBusinessCommit > 0 && rows[rowIndex].StartTimestampMs > 0 {
			rows[rowIndex].PhaseSyncMillis = nonNegativeMillis(rows[rowIndex].StartTimestampMs - prevBusinessCommit)
		}
	}

	for _, indices := range byShard {
		for pos, rowIndex := range indices {
			if pos%businessBatch != 0 {
				continue
			}
			phaseIndex := pos / businessBatch
			if phaseIndex == 0 {
				continue
			}
			prevBridgeCommit := bridgePhaseLastCommit[phaseIndex-1]
			if prevBridgeCommit > 0 && rows[rowIndex].StartTimestampMs > 0 {
				rows[rowIndex].PhaseSyncMillis = nonNegativeMillis(rows[rowIndex].StartTimestampMs - prevBridgeCommit)
			}
		}
	}
}

func blockRowShards(rows []BlockRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		if row.Shard != "" {
			seen[row.Shard] = struct{}{}
		}
	}
	shards := make([]string, 0, len(seen))
	for shard := range seen {
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool {
		return shardSortKey(shards[i]) < shardSortKey(shards[j])
	})
	return shards
}

func sortScheduleMetricEvents(events []scheduleMetricEvent) {
	sort.SliceStable(events, func(i, j int) bool {
		if !events[i].At.Equal(events[j].At) {
			return events[i].At.Before(events[j].At)
		}
		return events[i].Shard < events[j].Shard
	})
}

func latestEventBefore(events []scheduleMetricEvent, at time.Time) (scheduleMetricEvent, bool) {
	var found scheduleMetricEvent
	ok := false
	for _, event := range events {
		if event.At.After(at) {
			break
		}
		found = event
		ok = true
	}
	return found, ok
}

func latestBusinessPhaseEndBefore(byShard map[string][]scheduleMetricEvent, shards []string, at time.Time) (string, time.Time, bool) {
	if len(shards) == 0 {
		return "", time.Time{}, false
	}
	parts := make([]string, 0, len(shards))
	var latest time.Time
	for _, shard := range shards {
		event, ok := latestEventBefore(byShard[shard], at)
		if !ok {
			return "", time.Time{}, false
		}
		height, _ := metricUint(event.Fields, "height")
		parts = append(parts, fmt.Sprintf("%s:%d", shard, height))
		if latest.IsZero() || event.At.After(latest) {
			latest = event.At
		}
	}
	return strings.Join(parts, ","), latest, !latest.IsZero()
}

func isMeasuredSchedulerBlock(row BlockRow) bool {
	if !row.SchedulerCommit || row.StartTimestampMs <= 0 || row.CommitTimestampMs <= 0 {
		return false
	}
	if row.SpecialBlock {
		return false
	}
	return performancePhase(row.Shard) != ""
}

func schedulerBlockPosition(rows []BlockRow, indices []int, target int) int {
	for pos, rowIndex := range indices {
		if rowIndex == target {
			return pos
		}
	}
	return -1
}

func nonNegativeMillis(ms int64) int64 {
	if ms < 0 {
		return 0
	}
	return ms
}

func runChainMetrics(rows []BlockRow) (int64, float64) {
	if len(rows) == 0 {
		return 0, 0
	}
	var userTx uint64
	for _, row := range rows {
		userTx += row.UserTxCount
	}
	duration := timestampDiffMillis(rows[0].Timestamp, rows[len(rows)-1].Timestamp)
	if duration <= 0 {
		return duration, 0
	}
	return duration, float64(userTx) / (float64(duration) / 1000)
}

func calculatePerformanceMetrics(rt Runtime, results []SubmitResult, rows []BlockRow) PerformanceMetrics {
	var metrics PerformanceMetrics
	var confirmed int64
	var totalLatency int64
	var firstStarted time.Time
	var lastConfirmed time.Time
	for _, result := range results {
		if !result.Confirmed || result.StartedAt.IsZero() || result.ConfirmedAt.IsZero() {
			continue
		}
		confirmed++
		if firstStarted.IsZero() || result.StartedAt.Before(firstStarted) {
			firstStarted = result.StartedAt
		}
		if lastConfirmed.IsZero() || result.ConfirmedAt.After(lastConfirmed) {
			lastConfirmed = result.ConfirmedAt
		}
		latency := result.ConfirmedAt.Sub(result.StartedAt).Milliseconds()
		if latency < 0 {
			latency = 0
		}
		totalLatency += latency
		if latency > metrics.MaxLatencyMs {
			metrics.MaxLatencyMs = latency
		}
	}
	if confirmed > 0 {
		metrics.AvgLatencyMs = float64(totalLatency) / float64(confirmed)
	}
	if confirmed > 0 && !firstStarted.IsZero() && !lastConfirmed.IsZero() {
		duration := lastConfirmed.Sub(firstStarted).Seconds()
		if duration > 0 {
			metrics.TPS = float64(confirmed) / duration
		}
	}
	metrics.PeakTPS = peakTPSByPeriod(rt, rows)
	return metrics
}

func cloneBlockRows(rows []BlockRow) []BlockRow {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([]BlockRow, len(rows))
	copy(cloned, rows)
	return cloned
}

func peakTPSByPeriod(rt Runtime, rows []BlockRow) float64 {
	if len(rows) == 0 {
		return 0
	}
	businessBatch := rt.Config.IntraConsensusRounds
	if businessBatch <= 0 {
		businessBatch = 5
	}
	bridgeBatch := rt.Config.InterConsensusRounds
	if bridgeBatch <= 0 {
		bridgeBatch = 8
	}
	if businessBatch <= 0 || bridgeBatch <= 0 {
		return 0
	}

	businessShards := make([]string, 0, rt.Config.BusinessShards)
	for i := 1; i <= rt.Config.BusinessShards; i++ {
		businessShards = append(businessShards, fmt.Sprintf("business-%d", i))
	}
	if len(businessShards) == 0 {
		for _, row := range rows {
			if performancePhase(row.Shard) == "business" && row.Shard != "" {
				businessShards = append(businessShards, row.Shard)
			}
		}
		sort.Slice(businessShards, func(i, j int) bool {
			return shardSortKey(businessShards[i]) < shardSortKey(businessShards[j])
		})
	}

	byBusinessShard := make(map[string][]BlockRow)
	bridgeRows := make([]BlockRow, 0)
	for _, row := range rows {
		if !isPeakCycleBlock(row) {
			continue
		}
		switch performancePhase(row.Shard) {
		case "business":
			byBusinessShard[row.Shard] = append(byBusinessShard[row.Shard], row)
		case "bridge":
			bridgeRows = append(bridgeRows, row)
		}
	}
	for shard := range byBusinessShard {
		sort.SliceStable(byBusinessShard[shard], func(i, j int) bool {
			return byBusinessShard[shard][i].Height < byBusinessShard[shard][j].Height
		})
	}
	sort.SliceStable(bridgeRows, func(i, j int) bool {
		return bridgeRows[i].Height < bridgeRows[j].Height
	})

	cycleCount := len(bridgeRows) / bridgeBatch
	for _, shard := range businessShards {
		shardCycleCount := len(byBusinessShard[shard]) / businessBatch
		if shardCycleCount < cycleCount {
			cycleCount = shardCycleCount
		}
	}
	if cycleCount <= 0 {
		return 0
	}

	buckets := make([]*periodPerfBucket, 0, cycleCount)
	for cycle := 0; cycle < cycleCount; cycle++ {
		bucket := &periodPerfBucket{Index: uint64(cycle + 1), HasBusiness: true, HasBridge: true}
		for _, shard := range businessShards {
			shardRows := byBusinessShard[shard]
			start := cycle * businessBatch
			end := start + businessBatch
			if end > len(shardRows) {
				bucket = nil
				break
			}
			for pos, row := range shardRows[start:end] {
				if pos == 0 {
					startTs := blockRowStartMillis(row)
					if startTs <= 0 {
						bucket = nil
						break
					}
					if bucket.StartBlockTs == 0 || startTs < bucket.StartBlockTs {
						bucket.StartBlockTs = startTs
					}
				}
				bucket.TxCount += row.UserTxCount
			}
			if bucket == nil {
				break
			}
		}
		if bucket == nil {
			continue
		}
		bridgeStart := cycle * bridgeBatch
		bridgeEnd := bridgeStart + bridgeBatch
		if bridgeEnd > len(bridgeRows) {
			continue
		}
		for _, row := range bridgeRows[bridgeStart:bridgeEnd] {
			endTs := blockRowEndMillis(row)
			if endTs <= 0 {
				bucket = nil
				break
			}
			if bucket.EndBlockTs == 0 || endTs > bucket.EndBlockTs {
				bucket.EndBlockTs = endTs
			}
			bucket.TxCount += row.UserTxCount
		}
		if bucket != nil {
			buckets = append(buckets, bucket)
		}
	}
	return peakTPSFromBlockBuckets(buckets)
}

func isPeakCycleBlock(row BlockRow) bool {
	if row.SpecialBlock || !row.SchedulerCommit {
		return false
	}
	return performancePhase(row.Shard) != ""
}

func blockRowStartMillis(row BlockRow) int64 {
	if row.StartTimestampMs > 0 {
		return row.StartTimestampMs
	}
	return blockRowEndMillis(row)
}

func blockRowEndMillis(row BlockRow) int64 {
	if row.CommitTimestampMs > 0 {
		return row.CommitTimestampMs
	}
	return timestampToUnixMillis(row.Timestamp)
}

func peakTPSFromBlockBuckets(buckets []*periodPerfBucket) float64 {
	var peak float64
	for _, bucket := range buckets {
		if bucket == nil || bucket.TxCount == 0 || !bucket.HasBusiness || !bucket.HasBridge {
			continue
		}
		duration := bucket.EndBlockTs - bucket.StartBlockTs
		if duration <= 0 {
			duration = 1000
		}
		tps := float64(bucket.TxCount) / (float64(duration) / 1000)
		if tps > peak {
			peak = tps
		}
	}
	return peak
}

func loadBlockProductionTimings(rt Runtime, shardKeys []string) map[blockTimingKey]blockTiming {
	timings := make(map[blockTimingKey]blockTiming)
	for _, shard := range shardKeys {
		files := systemLogFilesForShard(rt, shard)
		if len(files) == 0 {
			continue
		}
		sort.Strings(files)
		for _, path := range files {
			parseBlockTimingLog(path, shard, timings)
		}
	}
	return timings
}

func loadScheduleMetricEvents(rt Runtime, shardKeys []string) []scheduleMetricEvent {
	events := make([]scheduleMetricEvent, 0)
	for _, shard := range shardKeys {
		files := systemLogFilesForShard(rt, shard)
		if len(files) == 0 {
			continue
		}
		sort.Strings(files)
		for _, path := range files {
			events = append(events, parseScheduleMetricEvents(path, shard)...)
		}
	}
	sort.SliceStable(events, func(i, j int) bool {
		if !events[i].At.Equal(events[j].At) {
			return events[i].At.Before(events[j].At)
		}
		if events[i].Shard != events[j].Shard {
			return shardSortKey(events[i].Shard) < shardSortKey(events[j].Shard)
		}
		return events[i].Event < events[j].Event
	})
	return events
}

func parseScheduleMetricEvents(path, shard string) []scheduleMetricEvent {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	events := make([]scheduleMetricEvent, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, schedMetricPrefix) {
			continue
		}
		ts, ok := parseLogTimestamp(line)
		if !ok {
			continue
		}
		event, ok := parseScheduleMetricLine(line, shard, ts)
		if ok {
			events = append(events, event)
		}
	}
	return events
}

func systemLogFilesForShard(rt Runtime, shard string) []string {
	if rt.Config.ChainmakerDir == "" {
		return nil
	}
	releaseDir := ""
	if shard == "bridge" {
		releaseDir = "bridge-shard"
	} else if strings.HasPrefix(shard, "business-") {
		releaseDir = "business-shard" + strings.TrimPrefix(shard, "business-")
	}
	if releaseDir == "" {
		return nil
	}
	patterns := []string{
		filepath.Join(rt.Config.ChainmakerDir, "build", "release", releaseDir, "chainmaker-*", "log", "system.log"),
		filepath.Join(rt.Config.ChainmakerDir, "build", "release", releaseDir, "chainmaker-*", "log", "system.log.*"),
	}
	seen := make(map[string]struct{})
	files := make([]string, 0)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, match := range matches {
			key := match
			if realPath, err := filepath.EvalSymlinks(match); err == nil {
				key = realPath
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			files = append(files, match)
		}
	}
	return files
}

func parseBlockTimingLog(path, shard string, timings map[blockTimingKey]blockTiming) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		ts, ok := parseLogTimestamp(line)
		if !ok {
			continue
		}
		if strings.Contains(line, "commit block [") {
			match := commitBlockLogRe.FindStringSubmatch(line)
			if match != nil {
				height, err := strconv.ParseUint(match[1], 10, 64)
				if err == nil {
					setBlockTimingIntervalCommit(timings, blockTimingKey{Shard: shard, Height: height}, ts)
				}
			}
		}
		if !strings.Contains(line, schedMetricPrefix) {
			continue
		}
		if event, ok := parseScheduleMetricLine(line, shard, ts); ok {
			applyScheduleMetricTimingEvent(timings, event)
		}
	}
}

func setBlockTimingIntervalCommit(timings map[blockTimingKey]blockTiming, key blockTimingKey, commit time.Time) {
	if commit.IsZero() {
		return
	}
	timing := timings[key]
	if timing.IntervalCommitAt.IsZero() || commit.Before(timing.IntervalCommitAt) {
		timing.IntervalCommitAt = commit
	}
	timings[key] = timing
}

func setBlockTimingConsensusStart(timings map[blockTimingKey]blockTiming, key blockTimingKey, start time.Time) {
	if start.IsZero() {
		return
	}
	timing := timings[key]
	if timing.ConsensusStartAt.IsZero() || start.Before(timing.ConsensusStartAt) {
		timing.ConsensusStartAt = start
	}
	timings[key] = timing
}

func markBlockTimingSpecial(timings map[blockTimingKey]blockTiming, key blockTimingKey) {
	timing := timings[key]
	timing.Special = true
	timings[key] = timing
}

func setBlockTimingStart(timings map[blockTimingKey]blockTiming, key blockTimingKey, start time.Time, source string) {
	if start.IsZero() {
		return
	}
	timing := timings[key]
	switch {
	case timing.AllowAt.IsZero():
		timing.AllowAt = start
		timing.StartSource = source
	case source == blockMetadataStartSource && timing.StartSource != blockMetadataStartSource:
		// The block metadata timestamp is carried by the block and is authoritative.
		timing.AllowAt = start
		timing.StartSource = source
	case source != blockMetadataStartSource && timing.StartSource == blockMetadataStartSource:
		// Do not let the local scheduler fallback replace authoritative metadata.
	case start.Before(timing.AllowAt):
		timing.AllowAt = start
		timing.StartSource = source
	}
	refreshBlockTimingDuration(&timing)
	timings[key] = timing
}

func setBlockTimingCommit(timings map[blockTimingKey]blockTiming, key blockTimingKey, commit time.Time, source string) {
	if commit.IsZero() {
		return
	}
	timing := timings[key]
	switch {
	case timing.CommitAt.IsZero():
		timing.CommitAt = commit
		timing.CommitSource = source
	case source == localCommitCompleteSource && timing.CommitSource != localCommitCompleteSource:
		// block_commit_complete is emitted after local ledger commit finishes.
		timing.CommitAt = commit
		timing.CommitSource = source
	case source != localCommitCompleteSource && timing.CommitSource == localCommitCompleteSource:
		// Do not downgrade a complete local commit to the scheduler fallback.
	case commit.After(timing.CommitAt):
		timing.CommitAt = commit
		timing.CommitSource = source
	}
	refreshBlockTimingDuration(&timing)
	timings[key] = timing
}

func isSchedulerCommitSource(source string) bool {
	return source == "sched_metric" || source == localCommitCompleteSource
}

func refreshBlockTimingDuration(timing *blockTiming) {
	if timing == nil {
		return
	}
	timing.DurationMillis = -1
	if timing.AllowAt.IsZero() || timing.CommitAt.IsZero() {
		return
	}
	timing.DurationMillis = timing.CommitAt.Sub(timing.AllowAt).Milliseconds()
	if timing.DurationMillis < 0 {
		timing.DurationMillis = 0
	}
}

func parseScheduleMetricLine(line, shard string, ts time.Time) (scheduleMetricEvent, bool) {
	idx := strings.Index(line, schedMetricPrefix)
	if idx < 0 {
		return scheduleMetricEvent{}, false
	}
	fields := make(map[string]string)
	payload := strings.TrimSpace(line[idx+len(schedMetricPrefix):])
	for _, part := range strings.Fields(payload) {
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			continue
		}
		fields[key] = value
	}
	event := fields["event"]
	if event == "" {
		return scheduleMetricEvent{}, false
	}
	return scheduleMetricEvent{
		Shard:  shard,
		Event:  event,
		At:     ts,
		Fields: fields,
	}, true
}

func metricUint(fields map[string]string, key string) (uint64, bool) {
	if fields == nil {
		return 0, false
	}
	value := strings.TrimSpace(fields[key])
	if value == "" || value == "-" {
		return 0, false
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func metricInt(fields map[string]string, key string) (int, bool) {
	if fields == nil {
		return 0, false
	}
	value := strings.TrimSpace(fields[key])
	if value == "" || value == "-" {
		return 0, false
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return n, true
}

func metricInt64(fields map[string]string, key string) (int64, bool) {
	if fields == nil {
		return 0, false
	}
	value := strings.TrimSpace(fields[key])
	if value == "" || value == "-" {
		return 0, false
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func metricBool(fields map[string]string, key string) bool {
	if fields == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(fields[key])) {
	case "true", "1", "yes", "y":
		return true
	default:
		return false
	}
}

func parseLogTimestamp(line string) (time.Time, bool) {
	tab := strings.IndexByte(line, '\t')
	if tab <= 0 {
		return time.Time{}, false
	}
	value := strings.TrimSpace(line[:tab])
	ts, err := time.ParseInLocation("2006-01-02 15:04:05.000", value, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func performancePhase(shard string) string {
	if shard == "bridge" {
		return "bridge"
	}
	if strings.HasPrefix(shard, "business-") {
		return "business"
	}
	return ""
}

func formatShardHeights(shards []string, heights map[string]uint64, limit int) string {
	if len(shards) == 0 {
		return "-"
	}
	if limit <= 0 || limit > len(shards) {
		limit = len(shards)
	}
	parts := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		shard := shards[i]
		parts = append(parts, fmt.Sprintf("%s=%d", shard, heights[shard]))
	}
	if len(shards) > limit {
		parts = append(parts, fmt.Sprintf("...+%d", len(shards)-limit))
	}
	return strings.Join(parts, ",")
}

func reportFromRunRows(rt Runtime, rows []BlockRow, durationMillis int64, committedTPSUser float64) ReportResult {
	report := ReportResult{
		GeneratedAt:      time.Now().Format(time.RFC3339Nano),
		BusinessShards:   rt.Config.BusinessShards,
		NodeID:           rt.Server.NodeID,
		RPCHost:          rt.Server.RPCHost,
		DurationMillis:   durationMillis,
		CommittedTPSUser: committedTPSUser,
		PerShard:         make(map[string]ShardSummary),
	}
	for _, row := range rows {
		report.BlockCount++
		report.TotalTxCount += row.TotalTxCount
		report.UserTxCount += row.UserTxCount
		report.CrossUserTxCount += row.CrossUserTxCount
		report.IntraUserTxCount += row.IntraUserTxCount
		report.InternalTxCount += row.InternalTxCount

		summary := report.PerShard[row.Shard]
		if summary.BlockCount == 0 {
			if row.Height > 0 {
				summary.StartHeight = row.Height - 1
			}
			summary.FirstTimestamp = row.Timestamp
		}
		summary.EndHeight = row.Height
		summary.BlockCount++
		summary.TotalTxCount += row.TotalTxCount
		summary.UserTxCount += row.UserTxCount
		summary.CrossUserTxCount += row.CrossUserTxCount
		summary.IntraUserTxCount += row.IntraUserTxCount
		summary.InternalTxCount += row.InternalTxCount
		summary.LastTimestamp = row.Timestamp
		report.PerShard[row.Shard] = summary
	}
	if report.DurationMillis > 0 {
		report.CommittedTPSTotal = float64(report.TotalTxCount) / (float64(report.DurationMillis) / 1000)
	}
	if report.BlockCount > 0 {
		report.AverageBlockTxs = float64(report.TotalTxCount) / float64(report.BlockCount)
		report.AverageBlockUserTx = float64(report.UserTxCount) / float64(report.BlockCount)
	}
	for shard, summary := range report.PerShard {
		summary.DurationMillis = timestampDiffMillis(summary.FirstTimestamp, summary.LastTimestamp)
		if summary.DurationMillis > 0 {
			seconds := float64(summary.DurationMillis) / 1000
			summary.CommittedTPSTotal = float64(summary.TotalTxCount) / seconds
			summary.CommittedTPSUser = float64(summary.UserTxCount) / seconds
		}
		report.PerShard[shard] = summary
	}
	return report
}

func collectBlocks(rt Runtime, clients *ClientSet, start Snapshot, end Snapshot, startPath, endPath string, includeEmpty bool) ([]BlockRow, ReportResult, error) {
	rows := make([]BlockRow, 0)
	shards := allShards(rt.Config.BusinessShards)
	shardKeys := make([]string, 0, len(shards))
	for _, shard := range shards {
		shardKeys = append(shardKeys, shard.Key)
	}
	blockTimings := loadBlockProductionTimings(rt, shardKeys)
	report := ReportResult{
		GeneratedAt:    time.Now().Format(time.RFC3339Nano),
		StartSnapshot:  startPath,
		EndSnapshot:    endPath,
		BusinessShards: rt.Config.BusinessShards,
		NodeID:         rt.Server.NodeID,
		RPCHost:        rt.Server.RPCHost,
		PerShard:       make(map[string]ShardSummary),
	}
	var firstGlobalTs int64
	var lastGlobalTs int64
	for _, shard := range shards {
		startHeight := start.Heights[shard.Key]
		endHeight := end.Heights[shard.Key]
		summary := ShardSummary{StartHeight: startHeight, EndHeight: endHeight}
		if endHeight <= startHeight {
			report.PerShard[shard.Key] = summary
			continue
		}
		cc, err := clients.Client(shard.Key)
		if err != nil {
			return nil, report, err
		}
		for h := startHeight + 1; h <= endHeight; h++ {
			blockInfo, err := cc.GetBlockByHeight(h, false)
			if err != nil {
				return nil, report, fmt.Errorf("get block %s/%d: %w", shard.Key, h, err)
			}
			if blockInfo == nil || blockInfo.Block == nil || blockInfo.Block.Header == nil {
				continue
			}
			header := blockInfo.Block.Header
			totalTx := uint64(header.TxCount)
			if totalTx == 0 && len(blockInfo.Block.Txs) > 0 {
				totalTx = uint64(len(blockInfo.Block.Txs))
			}
			userTx, crossTx, intraTx := countUserTxs(blockInfo.Block.Txs)
			internalTx := uint64(0)
			if totalTx >= userTx {
				internalTx = totalTx - userTx
			}
			if totalTx == 0 && !includeEmpty {
				continue
			}
			ts := header.BlockTimestamp
			startTimestampMs := int64(0)
			startSource := ""
			consensusStartMs := int64(0)
			commitTimestampMs := int64(0)
			intervalCommitMs := int64(0)
			blockTimeMillis := int64(-1)
			blockTimeSource := ""
			specialBlock := false
			if timing, ok := blockTimings[blockTimingKey{Shard: shard.Key, Height: h}]; ok {
				if !timing.AllowAt.IsZero() {
					startTimestampMs = timing.AllowAt.UnixMilli()
				}
				startSource = timing.StartSource
				specialBlock = timing.Special
				if !timing.ConsensusStartAt.IsZero() {
					consensusStartMs = timing.ConsensusStartAt.UnixMilli()
				}
				if !timing.CommitAt.IsZero() {
					commitTimestampMs = timing.CommitAt.UnixMilli()
				}
				if !timing.IntervalCommitAt.IsZero() {
					intervalCommitMs = timing.IntervalCommitAt.UnixMilli()
				}
				blockTimeMillis = timing.DurationMillis
				blockTimeSource = blockTimingSource(timing)
			}
			if summary.BlockCount == 0 {
				summary.FirstTimestamp = ts
			}
			if firstGlobalTs == 0 || timestampBefore(ts, firstGlobalTs) {
				firstGlobalTs = ts
			}
			if lastGlobalTs == 0 || timestampBefore(lastGlobalTs, ts) {
				lastGlobalTs = ts
			}
			row := BlockRow{
				Shard:             shard.Key,
				Height:            h,
				BlockHash:         fmt.Sprintf("%x", header.BlockHash),
				Timestamp:         ts,
				StartTimestampMs:  startTimestampMs,
				StartSource:       startSource,
				SpecialBlock:      specialBlock,
				SchedulerCommit:   isSchedulerCommitSource(blockTimings[blockTimingKey{Shard: shard.Key, Height: h}].CommitSource),
				ConsensusStartMs:  consensusStartMs,
				CommitTimestampMs: commitTimestampMs,
				BlockTimeMillis:   blockTimeMillis,
				BlockTimeSource:   blockTimeSource,
				IntervalCommitMs:  intervalCommitMs,
				BlockIntervalMs:   -1,
				PhaseSyncMillis:   -1,
				TotalTxCount:      totalTx,
				UserTxCount:       userTx,
				CrossUserTxCount:  crossTx,
				IntraUserTxCount:  intraTx,
				InternalTxCount:   internalTx,
			}
			rows = append(rows, row)
			summary.BlockCount++
			summary.TotalTxCount += totalTx
			summary.UserTxCount += userTx
			summary.CrossUserTxCount += crossTx
			summary.IntraUserTxCount += intraTx
			summary.InternalTxCount += internalTx
			summary.LastTimestamp = ts
		}
		summary.DurationMillis = timestampDiffMillis(summary.FirstTimestamp, summary.LastTimestamp)
		if summary.DurationMillis > 0 {
			seconds := float64(summary.DurationMillis) / 1000
			summary.CommittedTPSTotal = float64(summary.TotalTxCount) / seconds
			summary.CommittedTPSUser = float64(summary.UserTxCount) / seconds
		}
		report.PerShard[shard.Key] = summary
		report.BlockCount += summary.BlockCount
		report.TotalTxCount += summary.TotalTxCount
		report.UserTxCount += summary.UserTxCount
		report.CrossUserTxCount += summary.CrossUserTxCount
		report.IntraUserTxCount += summary.IntraUserTxCount
		report.InternalTxCount += summary.InternalTxCount
	}
	report.DurationMillis = timestampDiffMillis(firstGlobalTs, lastGlobalTs)
	if report.DurationMillis > 0 {
		seconds := float64(report.DurationMillis) / 1000
		report.CommittedTPSTotal = float64(report.TotalTxCount) / seconds
		report.CommittedTPSUser = float64(report.UserTxCount) / seconds
	}
	if report.BlockCount > 0 {
		report.AverageBlockTxs = float64(report.TotalTxCount) / float64(report.BlockCount)
		report.AverageBlockUserTx = float64(report.UserTxCount) / float64(report.BlockCount)
	}
	fillPhaseSync(rt, rows)
	sortBlockRows(rows)
	fillBlockRowCumulative(rows)
	return rows, report, nil
}

func countUserTxs(txs []*pbcommon.Transaction) (user, cross, intra uint64) {
	for _, tx := range txs {
		if tx == nil || tx.Payload == nil {
			continue
		}
		isUser, isCross := classifyPayload(tx.Payload)
		if !isUser {
			continue
		}
		user++
		if isCross {
			cross++
		} else {
			intra++
		}
	}
	return user, cross, intra
}

func classifyPayload(payload *pbcommon.Payload) (bool, bool) {
	if payload == nil {
		return false, false
	}
	contract := strings.TrimSpace(payload.ContractName)
	if !isSupportedWorkloadContract(contract) {
		return false, false
	}
	method := strings.TrimSpace(payload.Method)
	expectedMethod := workloadMethod(contract)
	isTransfer := method == expectedMethod
	if method == "invoke_contract" && txParam(payload.Parameters, "method") == expectedMethod {
		isTransfer = true
	}
	if !isTransfer {
		return false, false
	}
	fromShard := txParam(payload.Parameters, "from_shard")
	toShard := txParam(payload.Parameters, "to_shard")
	isCross := txParam(payload.Parameters, "__cross_shard") == "true" || (fromShard != "" && toShard != "" && fromShard != toShard)
	return true, isCross
}

func writeBlockCSV(path string, rows []BlockRow, report ReportResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"row_type", "shard", "height", "block_timestamp", "block_time_seconds", "block_time_source", "block_interval_seconds", "block_interval_source", "allow_time", "commit_time", "elapsed_seconds_from_first",
		"total_tx_count", "user_tx_count", "cross_user_tx_count", "intra_user_tx_count", "internal_tx_count",
	}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write([]string{
			"BLOCK",
			row.Shard,
			strconv.FormatUint(row.Height, 10),
			strconv.FormatInt(row.Timestamp, 10),
			formatDurationSeconds(row.BlockTimeMillis),
			row.BlockTimeSource,
			formatDurationSeconds(row.BlockIntervalMs),
			row.BlockIntervalSrc,
			formatUnixMillis(row.StartTimestampMs),
			formatUnixMillis(row.CommitTimestampMs),
			formatDurationSeconds(row.ElapsedMillis),
			strconv.FormatUint(row.TotalTxCount, 10),
			strconv.FormatUint(row.UserTxCount, 10),
			strconv.FormatUint(row.CrossUserTxCount, 10),
			strconv.FormatUint(row.IntraUserTxCount, 10),
			strconv.FormatUint(row.InternalTxCount, 10),
		}); err != nil {
			return err
		}
	}
	if err := writer.Write([]string{
		"TOTAL", "", "", "", "", "", "", "", "", "", formatDurationSeconds(report.DurationMillis),
		strconv.FormatUint(report.TotalTxCount, 10),
		strconv.FormatUint(report.UserTxCount, 10),
		strconv.FormatUint(report.CrossUserTxCount, 10),
		strconv.FormatUint(report.IntraUserTxCount, 10),
		strconv.FormatUint(report.InternalTxCount, 10),
	}); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func writePhaseSyncCSV(path string, rows []BlockRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"row_type", "phase", "shard", "height", "block_timestamp", "phase_sync_seconds", "allow_time", "commit_time",
		"block_time_seconds", "block_time_source", "user_tx_count", "cross_user_tx_count", "intra_user_tx_count", "internal_tx_count",
	}); err != nil {
		return err
	}
	for _, row := range rows {
		if row.PhaseSyncMillis < 0 {
			continue
		}
		phase := "bridge_to_business"
		if row.Shard == "bridge" {
			phase = "business_to_bridge"
		}
		if err := writer.Write([]string{
			"PHASE_SYNC",
			phase,
			row.Shard,
			strconv.FormatUint(row.Height, 10),
			strconv.FormatInt(row.Timestamp, 10),
			formatDurationSeconds(row.PhaseSyncMillis),
			formatUnixMillis(row.StartTimestampMs),
			formatUnixMillis(blockRowEndMillis(row)),
			formatDurationSeconds(row.BlockTimeMillis),
			row.BlockTimeSource,
			strconv.FormatUint(row.UserTxCount, 10),
			strconv.FormatUint(row.CrossUserTxCount, 10),
			strconv.FormatUint(row.IntraUserTxCount, 10),
			strconv.FormatUint(row.InternalTxCount, 10),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeShardBlockCSVs(dir string, rows []BlockRow, shardKeys []string, rt Runtime) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	rowsByShard := make(map[string][]BlockRow)
	for _, row := range rows {
		rowsByShard[row.Shard] = append(rowsByShard[row.Shard], row)
	}
	for _, shard := range shardKeys {
		shardRows := cloneBlockRows(rowsByShard[shard])
		sortBlockRows(shardRows)
		fillBlockRowCumulative(shardRows)
		fillBlockRowIntervals(shardRows)
		durationMs, committedTPS := runChainMetrics(shardRows)
		report := reportFromRunRows(rt, shardRows, durationMs, committedTPS)
		path := filepath.Join(dir, shard, "blocks.csv")
		if err := writeBlockCSV(path, shardRows, report); err != nil {
			return fmt.Errorf("write shard blocks %s: %w", shard, err)
		}
	}
	return nil
}

func formatDurationSeconds(ms int64) string {
	if ms < 0 {
		return ""
	}
	return fmt.Sprintf("%.3f", float64(ms)/1000)
}

func formatUnixMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).Format(time.RFC3339Nano)
}

func writeTransactionsCSV(path string, results []SubmitResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"seq", "tx_id", "target_shard", "tx_type", "started_at", "submitted_at",
		"confirmed", "confirmed_at", "confirm_latency_ms", "block_shard", "block_height", "block_timestamp", "block_hash", "error",
	}); err != nil {
		return err
	}
	formatTime := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.Format(time.RFC3339Nano)
	}
	for _, result := range results {
		if err := writer.Write([]string{
			strconv.FormatInt(result.Seq, 10),
			result.TxID,
			result.TargetShard,
			result.TxType,
			formatTime(result.StartedAt),
			formatTime(result.SubmittedAt),
			strconv.FormatBool(result.Confirmed),
			formatTime(result.ConfirmedAt),
			strconv.FormatInt(result.ConfirmLatencyMs, 10),
			result.BlockShard,
			strconv.FormatUint(result.BlockHeight, 10),
			strconv.FormatInt(result.BlockTimestamp, 10),
			result.BlockHash,
			result.Error,
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeBlockRowsJSONL(path string, rows []BlockRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}

func readBlockRowsJSONL(path string) ([]BlockRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	rows := make([]BlockRow, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row BlockRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func readTransactionsCSV(path string) ([]SubmitResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	header := make(map[string]int)
	for i, name := range records[0] {
		header[name] = i
	}
	value := func(record []string, key string) string {
		idx, ok := header[key]
		if !ok || idx < 0 || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}
	results := make([]SubmitResult, 0, len(records)-1)
	for _, record := range records[1:] {
		seq, _ := strconv.ParseInt(value(record, "seq"), 10, 64)
		latency, _ := strconv.ParseInt(value(record, "confirm_latency_ms"), 10, 64)
		height, _ := strconv.ParseUint(value(record, "block_height"), 10, 64)
		blockTimestamp, _ := strconv.ParseInt(value(record, "block_timestamp"), 10, 64)
		confirmed, _ := strconv.ParseBool(value(record, "confirmed"))
		startedAt, _ := parseOptionalTime(value(record, "started_at"))
		submittedAt, _ := parseOptionalTime(value(record, "submitted_at"))
		confirmedAt, _ := parseOptionalTime(value(record, "confirmed_at"))
		results = append(results, SubmitResult{
			Seq:              seq,
			TxID:             value(record, "tx_id"),
			TargetShard:      value(record, "target_shard"),
			TxType:           value(record, "tx_type"),
			StartedAt:        startedAt,
			SubmittedAt:      submittedAt,
			Error:            value(record, "error"),
			Confirmed:        confirmed,
			ConfirmedAt:      confirmedAt,
			ConfirmLatencyMs: latency,
			BlockShard:       value(record, "block_shard"),
			BlockHeight:      height,
			BlockTimestamp:   blockTimestamp,
			BlockHash:        value(record, "block_hash"),
		})
	}
	return results, nil
}

func writeScheduleMetricEventsJSONL(path string, events []scheduleMetricEvent) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, event := range events {
		if err := enc.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func readScheduleMetricEventsDir(dir string) ([]scheduleMetricEvent, error) {
	events := make([]scheduleMetricEvent, 0)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry == nil || entry.IsDir() || filepath.Base(path) != "schedule_metrics.jsonl" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 1024), 10*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var event scheduleMetricEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			events = append(events, event)
		}
		return scanner.Err()
	})
	if err != nil {
		return nil, err
	}
	sortScheduleMetricEvents(events)
	return events, nil
}

func timingsFromScheduleMetricEvents(events []scheduleMetricEvent) map[blockTimingKey]blockTiming {
	timings := make(map[blockTimingKey]blockTiming)
	for _, event := range events {
		applyScheduleMetricTimingEvent(timings, event)
	}
	return timings
}

func applyScheduleMetricTimingEvent(
	timings map[blockTimingKey]blockTiming,
	event scheduleMetricEvent,
) {
	height, ok := metricUint(event.Fields, "height")
	if !ok || event.Shard == "" {
		return
	}
	key := blockTimingKey{Shard: event.Shard, Height: height}
	switch event.Event {
	case "allow_produce":
		setBlockTimingStart(timings, key, event.At, "sched_metric_allow")
		setBlockTimingConsensusStart(timings, key, event.At)
	case "block_commit":
		setBlockTimingCommit(timings, key, event.At, "sched_metric")
	case "block_commit_complete":
		startMillis, startOK := metricInt64(event.Fields, "start_ms")
		commitMillis, commitOK := metricInt64(event.Fields, "commit_ms")
		if startOK && commitOK && startMillis > 0 && commitMillis >= startMillis {
			start := time.UnixMilli(startMillis)
			commit := time.UnixMilli(commitMillis)
			setBlockTimingStart(timings, key, start, blockMetadataStartSource)
			setBlockTimingConsensusStart(timings, key, start)
			setBlockTimingCommit(timings, key, commit, localCommitCompleteSource)
		}
	}
	if metricBool(event.Fields, "special") {
		markBlockTimingSpecial(timings, key)
	}
}

func parseOptionalTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	if ts, err := time.ParseInLocation("2006-01-02 15:04:05.000", raw, time.Local); err == nil {
		return ts, nil
	}
	if ts, err := time.ParseInLocation("2006-01-02 15:04:05", raw, time.Local); err == nil {
		return ts, nil
	}
	return time.Time{}, fmt.Errorf("unsupported time format %q", raw)
}

func eventInWindow(at, since, until time.Time) bool {
	if at.IsZero() {
		return false
	}
	if !since.IsZero() && at.Before(since) {
		return false
	}
	if !until.IsZero() && at.After(until) {
		return false
	}
	return true
}

func filterMetricEventsForRun(events []scheduleMetricEvent, result ServerResult) []scheduleMetricEvent {
	since, _ := parseOptionalTime(result.StartedAt)
	until, _ := parseOptionalTime(result.MeasureFinishedAt)
	if since.IsZero() && until.IsZero() {
		return events
	}
	if !since.IsZero() {
		since = since.Add(-10 * time.Minute)
	}
	if !until.IsZero() {
		until = until.Add(10 * time.Minute)
	}
	filtered := make([]scheduleMetricEvent, 0, len(events))
	for _, event := range events {
		if eventInWindow(event.At, since, until) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func readJSON(path string, value interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func readSnapshot(path string) (Snapshot, error) {
	var snap Snapshot
	data, err := os.ReadFile(path)
	if err != nil {
		return snap, err
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return snap, err
	}
	if len(snap.Heights) == 0 {
		return snap, fmt.Errorf("snapshot has no heights: %s", path)
	}
	return snap, nil
}

func shardAdmins(rt Runtime, shard Shard) ([]AdminUser, error) {
	shardDir := shardDir(shard)
	admins := make([]AdminUser, 0, rt.Config.NodesPerShard)
	for i := 1; i <= rt.Config.NodesPerShard; i++ {
		orgID := fmt.Sprintf(defaultOrgPattern, i)
		base := filepath.Join(rt.Config.ChainmakerDir, "build", shardDir, "crypto-config", orgID, "user", "admin1")
		admins = append(admins, AdminUser{
			Name:        fmt.Sprintf("admin1-%s", orgID),
			OrgID:       orgID,
			SignKeyPath: filepath.Join(base, "admin1.sign.key"),
			SignCrtPath: filepath.Join(base, "admin1.sign.crt"),
		})
	}
	return admins, nil
}

func shardDir(shard Shard) string {
	if shard.Type == "bridge" || shard.Key == "bridge" {
		return "bridge-shard"
	}
	return fmt.Sprintf("business-shard%d", shard.ID)
}

func shardChainID(shard Shard) string {
	if shard.Type == "bridge" || shard.Key == "bridge" {
		return "chain0"
	}
	return fmt.Sprintf("chain%d", shard.ID)
}

func shardRPCPort(cfg Config, shard Shard, nodeID int) int {
	if shard.Type == "bridge" || shard.Key == "bridge" {
		return cfg.BaseRPCPort + cfg.BusinessShards*cfg.NodesPerShard + (nodeID - 1)
	}
	return cfg.BaseRPCPort + (shard.ID-1)*cfg.NodesPerShard + (nodeID - 1)
}

func parseShard(raw string, businessShards int) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	if raw == "bridge" || raw == "bridge-shard" {
		return "bridge"
	}
	raw = strings.TrimPrefix(raw, "business-shard")
	raw = strings.TrimPrefix(raw, "business-")
	id, err := strconv.Atoi(raw)
	if err != nil || id < 1 || id > businessShards {
		return ""
	}
	return fmt.Sprintf("business-%d", id)
}

func txParam(params []*pbcommon.KeyValuePair, key string) string {
	for _, kv := range params {
		if kv != nil && kv.Key == key {
			return strings.TrimSpace(string(kv.Value))
		}
	}
	return ""
}

func computeBusinessShardIndex(account string, shardNumber int) int {
	sum := sha256.Sum256([]byte(account))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(shardNumber))
}

func deriveDefaultAccount(shardIndex, shardNumber int) (string, error) {
	accounts, err := deriveShardAccounts(shardIndex, shardNumber, 1)
	if err != nil {
		return "", err
	}
	return accounts[0], nil
}

func deriveShardAccounts(shardIndex, shardNumber, count int) ([]string, error) {
	if shardNumber <= 0 || shardIndex < 0 || shardIndex >= shardNumber {
		return nil, fmt.Errorf("invalid shard config: index=%d number=%d", shardIndex, shardNumber)
	}
	if count <= 0 {
		return nil, fmt.Errorf("account count must be positive")
	}
	accounts := make([]string, 0, count)
	limit := count * shardNumber * 4
	if limit < 100000 {
		limit = 100000
	}
	for i := 0; i < limit && len(accounts) < count; i++ {
		account := fmt.Sprintf("biz_fixed_shard%d_%d", shardIndex, i)
		if computeBusinessShardIndex(account, shardNumber) == shardIndex {
			accounts = append(accounts, account)
		}
	}
	if len(accounts) < count {
		return nil, fmt.Errorf("cannot derive %d accounts for shard index=%d number=%d, got=%d",
			count, shardIndex, shardNumber, len(accounts))
	}
	return accounts, nil
}

func checkDockerGoBinary(contractPath string) error {
	if !strings.HasSuffix(contractPath, ".7z") {
		return nil
	}
	if _, err := os.Stat(contractPath); err != nil {
		return err
	}
	binaryPath := strings.TrimSuffix(contractPath, ".7z")
	if _, err := os.Stat(binaryPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	elfFile, err := elf.Open(binaryPath)
	if err != nil {
		return err
	}
	defer elfFile.Close()
	for _, prog := range elfFile.Progs {
		if prog.Type == elf.PT_INTERP {
			return fmt.Errorf("contract binary is dynamically linked: %s", binaryPath)
		}
	}
	return nil
}

func waitContractExists(client *sdk.ChainClient, contractName string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := client.GetContractInfo(contractName); err == nil {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func checkTxResponse(resp *pbcommon.TxResponse) error {
	if resp == nil {
		return fmt.Errorf("nil response")
	}
	if resp.Code != pbcommon.TxStatusCode_SUCCESS {
		return fmt.Errorf("code=%s msg=%s", resp.Code.String(), resp.Message)
	}
	if resp.ContractResult != nil && resp.ContractResult.Code != 0 {
		return fmt.Errorf("contract code=%d msg=%s", resp.ContractResult.Code, resp.ContractResult.Message)
	}
	return nil
}

func isSyncTimeout(resp *pbcommon.TxResponse, err error) bool {
	if resp != nil && resp.Code == pbcommon.TxStatusCode_TIMEOUT {
		return true
	}
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "sync_tx_result_timeout") || strings.Contains(msg, "request reached") || strings.Contains(msg, "TIMEOUT")
}

type namedJob struct {
	name string
	run  func() error
}

func runJobs(title string, jobs []namedJob, concurrency int) error {
	if len(jobs) == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(jobs) {
		concurrency = len(jobs)
	}
	fmt.Printf("===> %s: tasks=%d concurrency=%d\n", title, len(jobs), concurrency)
	jobCh := make(chan namedJob)
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if err := job.run(); err != nil {
					errCh <- fmt.Errorf("%s: %w", job.name, err)
					fmt.Printf("  x %s: %v\n", job.name, err)
					continue
				}
				fmt.Printf("  ok %s\n", job.name)
			}
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	wg.Wait()
	close(errCh)
	if len(errCh) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(errCh))
	for err := range errCh {
		msgs = append(msgs, err.Error())
	}
	sort.Strings(msgs)
	return errors.New(strings.Join(msgs, "; "))
}

func timestampToTime(ts int64) time.Time {
	if ts >= 1_000_000_000_000 {
		return time.UnixMilli(ts)
	}
	return time.Unix(ts, 0)
}

func timestampToUnixMillis(ts int64) int64 {
	if ts >= 1_000_000_000_000 {
		return ts
	}
	return ts * 1000
}

func timestampDiffMillis(start, end int64) int64 {
	if start <= 0 || end <= 0 {
		return 0
	}
	diff := timestampToTime(end).Sub(timestampToTime(start)).Milliseconds()
	if diff < 0 {
		return 0
	}
	return diff
}

func timestampBefore(a, b int64) bool {
	return timestampToTime(a).Before(timestampToTime(b))
}

func shardSortKey(shard string) int {
	if shard == "bridge" {
		return 100000
	}
	if strings.HasPrefix(shard, "business-") {
		id, err := strconv.Atoi(strings.TrimPrefix(shard, "business-"))
		if err == nil {
			return id
		}
	}
	return 99999
}

func writeJSON(path string, value interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func resolvePath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(base, path))
}

func sanitizeFileName(s string) string {
	return strings.NewReplacer("/", "_", "\\", "_", "-", "_").Replace(s)
}

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func effectiveSendConcurrency(rate, configured int) int {
	const (
		inFlightHeadroomMillis = 100
		maxSendConcurrency     = 16384
	)
	required := int((int64(rate)*inFlightHeadroomMillis + 999) / 1000)
	workers := maxInt(configured, required)
	return minInt(workers, maxSendConcurrency)
}
