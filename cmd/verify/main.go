package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"skiresort/tail-sampling/internal/sim"
)

func main() {
	seed := flag.Uint64("seed", 15833, "fixed random seed")
	duration := flag.Duration("duration", 3*time.Hour, "virtual simulated duration")
	maxDeviation := flag.Float64("max-deviation", 0.15, "allowed per-service cumulative RPS deviation, fraction")
	jsonOut := flag.String("json", "", "optional path to write full JSON report")
	flag.Parse()

	report, err := sim.Run(sim.Config{Seed: *seed, Duration: *duration, MaxDeviation: *maxDeviation})
	if err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(2)
	}

	fmt.Printf("滑雪场尾部采样核对报告  seed=%d 虚拟时长=%s 服务数=%d\n",
		report.Config.Seed, report.Config.Duration, report.ServiceCount)
	fmt.Printf("生成链路=%d 片段=%d；错误链路=%d 慢链路=%d 超大链路=%d；边界片段=%d 迟到片段=%d\n",
		report.GeneratedTraces, report.GeneratedSpans, report.ErrorTraces, report.SlowTraces,
		report.HugeTraces, report.StragglerScheduled, report.LateScheduled)
	fmt.Printf("决策=%d 保留=%d 丢弃=%d 总保留率=%.2f%%\n",
		report.Snapshot.Decisions.Total, report.Snapshot.Decisions.Keep, report.Snapshot.Decisions.Drop,
		report.Snapshot.Decisions.KeepRate*100)
	fmt.Printf("缓冲峰值=%d 条 / %.2f MiB（上限 %.0f MiB）；当前=%d 条 / %.2f MiB；Go HeapAlloc=%.2f MiB\n",
		report.Snapshot.Buffered.PeakTraces,
		float64(report.Snapshot.Buffered.PeakBytes)/1024/1024,
		float64(report.Snapshot.Buffered.CapacityBytes)/1024/1024,
		report.Snapshot.Buffered.Traces,
		float64(report.Snapshot.Buffered.EstimatedBytes)/1024/1024,
		float64(report.Snapshot.Buffered.GoHeapAlloc)/1024/1024)
	fmt.Printf("策略命中: ")
	for i, p := range report.Snapshot.Policies {
		if i > 0 {
			fmt.Printf(" | ")
		}
		fmt.Printf("%s=%d", p.Name, p.Hits)
	}
	fmt.Println()
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "服务\t目标保留/s\t实际保留/s\t偏差%\t概率\t到达/s\t保留数")
	for _, s := range report.ServiceReports {
		fmt.Fprintf(w, "%s\t%.3f\t%.3f\t%+.2f\t%.3f\t%d\t%d\n",
			s.Name, s.TargetRPS, s.ActualRPS, s.DeviationPct, s.Probability, s.Arrivals, s.Kept)
	}
	_ = w.Flush()
	fmt.Println()

	late := report.Snapshot.Late
	fmt.Printf("迟到处理: 总=%d 缓存保留=%d 缓存丢弃=%d 淘汰后独立保留=%d 淘汰后独立丢弃=%d（淘汰路径计数=%d）\n",
		late.Total, late.CachedKeep, late.CachedDrop, late.IndependentKeep, late.IndependentDrop, late.CacheEvicted)
	fmt.Printf("静默顺延=%d；提前批次=%d / %d 片段；sink不一致=%d\n\n",
		report.Snapshot.Extensions, report.Snapshot.Batches.Early, report.Snapshot.Batches.Spans,
		len(report.SinkReport.Mismatches))

	for _, c := range report.Checks {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
		}
		fmt.Printf("[%s] %s — %s\n", mark, c.Name, c.Detail)
	}
	if len(report.MissedMustKeep) > 0 {
		n := 10
		if len(report.MissedMustKeep) < n {
			n = len(report.MissedMustKeep)
		}
		fmt.Println("漏采样例:", report.MissedMustKeep[:n])
	}
	if len(report.SinkReport.Mismatches) > 0 {
		n := 10
		if len(report.SinkReport.Mismatches) < n {
			n = len(report.SinkReport.Mismatches)
		}
		fmt.Println("一致性错误样例:", report.SinkReport.Mismatches[:n])
	}

	if *jsonOut != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, "marshal report:", err)
			os.Exit(2)
		}
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write report:", err)
			os.Exit(2)
		}
		fmt.Println("JSON report written:", *jsonOut)
	}

	if !report.Pass {
		os.Exit(1)
	}
}
