package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/unix"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

type Config struct {
        LogStreamer LogStreamerConfig `toml:"logstreamer"`
        Exporter    ExporterConfig    `toml:"exporter"`
}

type LogStreamerConfig struct {
        LogFilePath            string   `toml:"logFilePath"`
        PollInterval           int      `toml:"pollInterval"` // in milliseconds
        WorkerCores            []int    `toml:"workerCores"`  // list of cores to pin workers
        Filters                []Filter `toml:"filters"`      // regex filters
        EnableOutputAggregator bool     `toml:"enable_output_aggregator"`
}

type ExporterConfig struct {
        EnableHTTP     bool   `toml:"enable_http"`
        HTTPPort       int    `toml:"http_port"`
        EnablePush     bool   `toml:"enable_push"`
        PushGatewayURL string `toml:"push_gateway_url"`
        PushInterval   int    `toml:"push_interval"` // in seconds
}

type Filter struct {
        Name     string `toml:"name"`
        Pattern  string `toml:"pattern"`
        Template string `toml:"template"`
        Regex    *regexp.Regexp
}

type job struct {
        seq  int
        line string
}

type result struct {
        seq       int
        processed string
}

func apply_template_named(tpl string, re *regexp.Regexp, match []string) string {
        names := re.SubexpNames()
        result := tpl
        for i, name := range names {
                if i == 0 || name == "" {
                        continue
                }
                result = strings.ReplaceAll(result, "${"+name+"}", match[i])
        }
        return result
}

func set_affinity(core int) error {
        runtime.LockOSThread()
        var mask unix.CPUSet
        mask.Zero()
        mask.Set(core)
        return unix.SchedSetaffinity(0, &mask)
}

var log_metrics = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
                Name: "logstreamer_metric",
                Help: "metrics extracted from log streamer",
        },
        []string{"filter", "field"},
)

func worker(worker_id int, core int, job_chan <-chan job, result_chan chan<- result, filters []Filter) {
        if err := set_affinity(core); err != nil {
                fmt.Printf("worker %d: error setting affinity to core %d: %v\n", worker_id, core, err)
        } else {
                fmt.Printf("worker %d: running on core %d\n", worker_id, core)
        }
        for job := range job_chan {
                matched := false
                var out string
                for _, f := range filters {
                        match := f.Regex.FindStringSubmatch(job.line)
                        if match != nil {
                                out = apply_template_named(f.Template, f.Regex, match)
                                names := f.Regex.SubexpNames()
                                for i, name := range names {
                                        if i == 0 || name == "" {
                                                continue
                                        }
                                        if value, err := strconv.ParseFloat(match[i], 64); err == nil {
                                                log_metrics.WithLabelValues(f.Name, name).Set(value)
                                        }
                                }
                                matched = true
                                break
                        }
                }
                var processed string
                if matched {
                        processed = fmt.Sprintf("worker %d processed: %s\n", worker_id, out)
                } else {
                        processed = ""
                }
                res := result{
                        seq:       job.seq,
                        processed: processed,
                }
                result_chan <- res
        }
}

func output_aggregator(result_chan <-chan result) {
        next_seq := 1
        buffer := make(map[int]result)
        for {
                res := <-result_chan
                if res.seq == next_seq {
                        if strings.TrimSpace(res.processed) != "" {
                                fmt.Printf("%s", res.processed)
                        }
                        next_seq++
                        for {
                                if r, ok := buffer[next_seq]; ok {
                                        if strings.TrimSpace(r.processed) != "" {
                                                fmt.Printf("%s", r.processed)
                                        }
                                        delete(buffer, next_seq)
                                        next_seq++
                                } else {
                                        break
                                }
                        }
                } else {
                        buffer[res.seq] = res
                }
        }
}

func start_http_exporter(port int) {
        addr := fmt.Sprintf(":%d", port)
        http.Handle("/metrics", promhttp.Handler())
        fmt.Printf("exporter http server running on %s\n", addr)
        if err := http.ListenAndServe(addr, nil); err != nil {
                fmt.Printf("error starting http server: %v\n", err)
        }
}

func start_push_exporter(push_url string, interval int) {
        ticker := time.NewTicker(time.Duration(interval) * time.Second)
        defer ticker.Stop()
        for {
                <-ticker.C
                err := push.New(push_url, "logstreamer_job").
                        Collector(log_metrics).
                        Push()
                if err != nil {
                        fmt.Printf("could not push metrics: %v\n", err)
                } else {
                        fmt.Printf("metrics pushed to %s\n", push_url)
                }
        }
}

func main() {
        var config Config
        if _, err := toml.DecodeFile("config.toml", &config); err != nil {
                fmt.Printf("error loading config: %v\n", err)
                return
        }
        log_file_path := config.LogStreamer.LogFilePath
        poll_interval := time.Duration(config.LogStreamer.PollInterval) * time.Millisecond

        for i := range config.LogStreamer.Filters {
                re, err := regexp.Compile(config.LogStreamer.Filters[i].Pattern)
                if err != nil {
                        fmt.Printf("error compiling filter %q: %v\n", config.LogStreamer.Filters[i].Name, err)
                        return
                }
                config.LogStreamer.Filters[i].Regex = re
        }

        prometheus.MustRegister(log_metrics)

        if config.Exporter.EnableHTTP {
                go start_http_exporter(config.Exporter.HTTPPort)
        }

        if config.Exporter.EnablePush && config.Exporter.PushGatewayURL != "" {
                go start_push_exporter(config.Exporter.PushGatewayURL, config.Exporter.PushInterval)
        }

        watcher, err := fsnotify.NewWatcher()
        if err != nil {
                fmt.Printf("error creating file watcher: %v\n", err)
                return
        }
        defer watcher.Close()

        log_dir := filepath.Dir(log_file_path)
        if err := watcher.Add(log_dir); err != nil {
                fmt.Printf("error watching directory %s: %v\n", log_dir, err)
                return
        }

        rotation_chan := make(chan bool, 1)
        go func() {
                for {
                        select {
                        case event, ok := <-watcher.Events:
                                if !ok {
                                        return
                                }
                                if filepath.Base(event.Name) == filepath.Base(log_file_path) {
                                        if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
                                                rotation_chan <- true
                                        }
                                }
                        case err, ok := <-watcher.Errors:
                                if !ok {
                                        return
                                }
                                fmt.Printf("watcher error: %v\n", err)
                        }
                }
        }()

        // in read-only-mode and jump to the end
        open_log_file := func() (*os.File, error) {
                f, err := os.Open(log_file_path)
                if err != nil {
                        return nil, err
                }
                _, err = f.Seek(0, io.SeekEnd)
                if err != nil {
                        f.Close()
                        return nil, err
                }
                return f, nil
        }

        file, err := open_log_file()
        if err != nil {
                fmt.Printf("error opening log file: %v\n", err)
                return
        }
        defer file.Close()
        reader := bufio.NewReader(file)

        job_chan := make(chan job, 100)
        result_chan := make(chan result, 100)

        if config.LogStreamer.EnableOutputAggregator {
                go output_aggregator(result_chan)
        }

        worker_cores := config.LogStreamer.WorkerCores
        if len(worker_cores) == 0 {
                worker_cores = []int{0}
        }
        for i, core := range worker_cores {
                go worker(i, core, job_chan, result_chan, config.LogStreamer.Filters)
        }

        seq := 1
        for {
                select {
                case <-rotation_chan:
                        file.Close()
                        time.Sleep(1 * time.Second)
                        new_file, err := open_log_file()
                        if err != nil {
                                fmt.Printf("error reopening log file: %v\n", err)
                                time.Sleep(1 * time.Second)
                                continue
                        }
                        file = new_file
                        reader = bufio.NewReader(file)
                default:
                }

                line, err := reader.ReadString('\n')
                if err != nil {
                        if err == io.EOF {
                                time.Sleep(poll_interval)
                                continue
                        }
                        fmt.Printf("error reading file: %v\n", err)
                        break
                }
                job_chan <- job{seq: seq, line: line}
                seq++
        }
}
