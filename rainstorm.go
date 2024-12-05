package main

import (
    "bufio"
    "bytes"
    "crypto/sha256"
    "encoding/csv"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "os/signal"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"
)

// 系统常量
const (
    DefaultBatchSize    = 100
    HeartbeatInterval   = 2 * time.Second
    FailureTimeout      = 10 * time.Second
    ProcessTimeout      = 5 * time.Second
    StateCheckInterval  = 30 * time.Second
    MaxRetries         = 3
    BasePort          = 5000
    FileServerPort    = 5001
    HeartbeatPort     = 5002
    LogBufferSize     = 1000
)

// 错误定义
var (
    ErrFileNotFound     = errors.New("file not found")
    ErrDuplicateTuple   = errors.New("duplicate tuple")
    ErrNodeNotFound     = errors.New("node not found")
    ErrInvalidOperation = errors.New("invalid operation")
    ErrTimeout         = errors.New("operation timed out")
)

// Message 类型定义
type MessageType string

const (
    MsgJoin        MessageType = "JOIN"
    MsgHeartbeat   MessageType = "HEARTBEAT"
    MsgStateSync   MessageType = "STATE_SYNC"
    MsgTuple       MessageType = "TUPLE"
    MsgBatch       MessageType = "BATCH"
    MsgAck         MessageType = "ACK"
    MsgError       MessageType = "ERROR"
)

// Message 结构体
type Message struct {
    Type      MessageType  `json:"type"`
    SenderID  string       `json:"sender_id"`
    Data      interface{}  `json:"data"`
    Timestamp time.Time    `json:"timestamp"`
}

// Tuple 定义
type Tuple struct {
    ID        string      `json:"id"`
    Key       string      `json:"key"`
    Value     interface{} `json:"value"`
    Timestamp time.Time   `json:"timestamp"`
    BatchID   string      `json:"batch_id"`
    Source    string      `json:"source"`
    Checksum  []byte      `json:"checksum"`
}

// Batch 定义
type Batch struct {
    ID            string          `json:"id"`
    Tuples        []*Tuple        `json:"tuples"`
    ProcessedIDs  map[string]bool `json:"processed_ids"`
    NextStageAcks map[string]bool `json:"next_stage_acks"`
    CreateTime    time.Time       `json:"create_time"`
    Checksum      []byte          `json:"checksum"`
}

// State 定义
type State struct {
    ProcessedTuples map[string]bool       `json:"processed_tuples"`
    AggregateState  map[string]interface{} `json:"aggregate_state"`
    LastSeqNum      int64                 `json:"last_seq_num"`
    LastBatchID     string                `json:"last_batch_id"`
}

// LogEntry 定义
type LogEntry struct {
    Timestamp time.Time    `json:"timestamp"`
    Level     string       `json:"level"`
    Type      string       `json:"type"`
    Message   string       `json:"message"`
    Data      interface{}  `json:"data,omitempty"`
}

// Worker 配置
type WorkerConfig struct {
    ID          string
    Role        string // "source", "transform", "aggregate"
    BatchSize   int
    ParamX      string
    HydfsAddr   string
    HydfsPort   int
    LogDir      string
}

// Worker 结构体
type Worker struct {
    Config         WorkerConfig
    ID             string
    Role           string
    State          *State
    CurrentBatch   *Batch
    InputChan      chan *Tuple
    OutputChan     chan *Tuple
    LogChan        chan *LogEntry
    StopChan       chan struct{}
    
    // 互斥锁
    StateMutex     sync.RWMutex
    BatchMutex     sync.RWMutex
    
    // 网络相关
    Listener       net.Listener
    Connections    map[string]net.Conn
    ConnMutex      sync.RWMutex
    
    // HyDFS客户端
    HydfsClient    *HydfsClient
    
    // 日志
    Logger         *log.Logger
    LogFile        *os.File
}

// HydfsClient 定义
type HydfsClient struct {
    Addr     string
    Port     int
    Conn     net.Conn
    Mutex    sync.Mutex
}

// NewHydfsClient 创建新的HyDFS客户端
func NewHydfsClient(addr string, port int) (*HydfsClient, error) {
    client := &HydfsClient{
        Addr: addr,
        Port: port,
    }
    
    // 建立连接
    if err := client.connect(); err != nil {
        return nil, err
    }
    
    return client, nil
}

// Worker 创建函数
func NewWorker(config WorkerConfig) (*Worker, error) {
    worker := &Worker{
        Config:      config,
        ID:          config.ID,
        Role:        config.Role,
        State:       &State{
            ProcessedTuples: make(map[string]bool),
            AggregateState:  make(map[string]interface{}),
        },
        InputChan:   make(chan *Tuple, DefaultBatchSize),
        OutputChan:  make(chan *Tuple, DefaultBatchSize),
        LogChan:     make(chan *LogEntry, LogBufferSize),
        StopChan:    make(chan struct{}),
        Connections: make(map[string]net.Conn),
    }
    
    // 初始化日志
    if err := worker.initLogger(); err != nil {
        return nil, err
    }
    
    // 初始化HyDFS客户端
    client, err := NewHydfsClient(config.HydfsAddr, config.HydfsPort)
    if err != nil {
        return nil, err
    }
    worker.HydfsClient = client
    
    return worker, nil
}

// Worker方法实现
func (w *Worker) Start() error {
    // 启动网络监听
    if err := w.startNetwork(); err != nil {
        return err
    }
    
    // 启动处理循环
    go w.processLoop()
    go w.logLoop()
    go w.stateSync()
    
    w.Logger.Printf("Worker started [Role: %s]", w.Role)
    return nil
}

func (w *Worker) Stop() {
    close(w.StopChan)
    w.Logger.Printf("Worker stopping...")
    
    // 关闭连接
    w.ConnMutex.Lock()
    for _, conn := range w.Connections {
        conn.Close()
    }
    w.ConnMutex.Unlock()
    
    if w.Listener != nil {
        w.Listener.Close()
    }
    
    // 关闭日志文件
    if w.LogFile != nil {
        w.LogFile.Close()
    }
}

// Worker具体处理方法实现
func (w *Worker) processLoop() {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-w.StopChan:
            return
        case tuple := <-w.InputChan:
            if err := w.processTuple(tuple); err != nil {
                w.logError("process_tuple_failed", err)
            }
        case <-ticker.C:
            w.checkAndSendBatch()
        }
    }
}

func (w *Worker) logLoop() {
    for {
        select {
        case <-w.StopChan:
            return
        case entry := <-w.LogChan:
            if err := w.writeLog(entry); err != nil {
                w.Logger.Printf("Failed to write log: %v", err)
            }
        }
    }
}

func (w *Worker) stateSync() {
    ticker := time.NewTicker(StateCheckInterval)
    defer ticker.Stop()

    for {
        select {
        case <-w.StopChan:
            return
        case <-ticker.C:
            if err := w.persistState(); err != nil {
                w.logError("state_sync_failed", err)
            }
        }
    }
}

func (w *Worker) processTuple(tuple *Tuple) error {
    // 检查重复
    tupleID := fmt.Sprintf("%s-%s", tuple.Source, tuple.ID)
    w.StateMutex.RLock()
    if w.State.ProcessedTuples[tupleID] {
        w.StateMutex.RUnlock()
        // 记录重复检测日志
        w.LogChan <- &LogEntry{
            Timestamp: time.Now(),
            Type:     "DUPLICATE_DETECTED",
            Message:  fmt.Sprintf("Duplicate tuple detected: %s", tupleID),
            Data:     tuple,
        }
        return nil
    }
    w.StateMutex.RUnlock()

    // 处理tuple
    var err error
    switch w.Role {
    case "source":
        err = w.handleSourceTuple(tuple)
    case "transform":
        err = w.handleTransformTuple(tuple)
    case "aggregate":
        err = w.handleAggregateTuple(tuple)
    }

    if err != nil {
        return err
    }

    // 标记为已处理
    w.StateMutex.Lock()
    w.State.ProcessedTuples[tupleID] = true
    w.State.LastSeqNum++
    w.StateMutex.Unlock()

    // 记录处理日志
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:     "TUPLE_PROCESSED",
        Message:  fmt.Sprintf("Processed tuple: %s", tupleID),
        Data:     tuple,
    }

    return nil
}

func (w *Worker) handleSourceTuple(tuple *Tuple) error {
    // 解析CSV行
    reader := csv.NewReader(strings.NewReader(tuple.Value.(string)))
    fields, err := reader.Read()
    if err != nil {
        return err
    }

    // 创建新tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       fields[0], // OBJECTID
        Value:     tuple.Value,
        Timestamp: time.Now(),
        Source:    w.ID,
        BatchID:   fmt.Sprintf("batch-%s-%d", w.ID, w.State.LastSeqNum/w.Config.BatchSize),
    }

    // 添加到当前批次
    return w.addToBatch(newTuple)
}

func (w *Worker) handleTransformTuple(tuple *Tuple) error {
    csvLine := tuple.Value.(string)
    reader := csv.NewReader(strings.NewReader(csvLine))
    fields, err := reader.Read()
    if err != nil {
        return err
    }

    // 根据Pattern X过滤
    if !strings.Contains(csvLine, w.Config.ParamX) {
        return nil
    }

    // 提取OBJECTID和Sign_Type
    if len(fields) < 4 {
        return fmt.Errorf("insufficient fields in CSV")
    }

    objectID := fields[0]
    signType := fields[3]

    // 创建新tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       objectID,
        Value:     fmt.Sprintf("%s,%s", objectID, signType),
        Timestamp: time.Now(),
        Source:    w.ID,
        BatchID:   tuple.BatchID,
    }

    return w.addToBatch(newTuple)
}

func (w *Worker) handleAggregateTuple(tuple *Tuple) error {
    csvLine := tuple.Value.(string)
    reader := csv.NewReader(strings.NewReader(csvLine))
    fields, err := reader.Read()
    if err != nil {
        return err
    }

    // 检查Sign Post类型
    if len(fields) < 5 {
        return fmt.Errorf("insufficient fields in CSV")
    }

    signPost := fields[3]
    if signPost != w.Config.ParamX {
        return nil
    }

    // 获取Category并规范化
    category := fields[4]
    if category == "" {
        category = "empty"
    } else if category == " " {
        category = "space"
    }

    // 更新计数
    w.StateMutex.Lock()
    if w.State.AggregateState[category] == nil {
        w.State.AggregateState[category] = 0
    }
    count := w.State.AggregateState[category].(int) + 1
    w.State.AggregateState[category] = count
    w.StateMutex.Unlock()

    // 创建新tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       category,
        Value:     count,
        Timestamp: time.Now(),
        Source:    w.ID,
        BatchID:   tuple.BatchID,
    }

    // 记录状态更新日志
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:     "STATE_UPDATE",
        Message:  fmt.Sprintf("Updated count for category %s: %d", category, count),
        Data: map[string]interface{}{
            "category": category,
            "count":   count,
        },
    }

    return w.addToBatch(newTuple)
}

func (w *Worker) addToBatch(tuple *Tuple) error {
    w.BatchMutex.Lock()
    defer w.BatchMutex.Unlock()

    if w.CurrentBatch == nil {
        w.CurrentBatch = &Batch{
            ID:            fmt.Sprintf("batch-%s-%d", w.ID, w.State.LastSeqNum/w.Config.BatchSize),
            Tuples:        make([]*Tuple, 0),
            ProcessedIDs:  make(map[string]bool),
            NextStageAcks: make(map[string]bool),
            CreateTime:    time.Now(),
        }
    }

    w.CurrentBatch.Tuples = append(w.CurrentBatch.Tuples, tuple)

    // 如果批次已满，发送它
    if len(w.CurrentBatch.Tuples) >= w.Config.BatchSize {
        return w.sendBatch(w.CurrentBatch)
    }

    return nil
}

func (w *Worker) checkAndSendBatch() {
    w.BatchMutex.Lock()
    defer w.BatchMutex.Unlock()

    if w.CurrentBatch != nil && len(w.CurrentBatch.Tuples) > 0 {
        if time.Since(w.CurrentBatch.CreateTime) >= ProcessTimeout {
            if err := w.sendBatch(w.CurrentBatch); err != nil {
                w.logError("send_batch_failed", err)
            }
        }
    }
}

func (w *Worker) sendBatch(batch *Batch) error {
    // 计算批次校验和
    batchData, err := json.Marshal(batch.Tuples)
    if err != nil {
        return err
    }
    batch.Checksum = calculateChecksum(batchData)

    // 发送到输出通道
    select {
    case w.OutputChan <- &Tuple{
        ID:        batch.ID,
        Value:     batch,
        Timestamp: time.Now(),
        Source:    w.ID,
    }:
        // 记录批次发送日志
        w.LogChan <- &LogEntry{
            Timestamp: time.Now(),
            Type:     "BATCH_SENT",
            Message:  fmt.Sprintf("Sent batch %s with %d tuples", batch.ID, len(batch.Tuples)),
            Data: map[string]interface{}{
                "batch_id":     batch.ID,
                "tuple_count":  len(batch.Tuples),
                "checksum":     fmt.Sprintf("%x", batch.Checksum),
            },
        }

        w.CurrentBatch = nil
        return nil
    case <-time.After(ProcessTimeout):
        return ErrTimeout
    }
}

// 实用函数
func calculateChecksum(data []byte) []byte {
    hash := sha256.New()
    hash.Write(data)
    return hash.Sum(nil)
}

func (w *Worker) logError(errType string, err error) {
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Level:    "ERROR",
        Type:     errType,
        Message:  err.Error(),
    }
}

func (w *Worker) writeLog(entry *LogEntry) error {
    // 写入本地日志文件
    logLine, err := json.Marshal(entry)
    if err != nil {
        return err
    }

    if _, err := fmt.Fprintln(w.LogFile, string(logLine)); err != nil {
        return err
    }

    // 同时写入HyDFS
    logPath := fmt.Sprintf("logs/%s/%s.log", w.ID, time.Now().Format("2006-01-02"))
    return w.HydfsClient.AppendFile(logPath, logLine)
}

// 状态同步和持久化实现
func (w *Worker) persistState() error {
    w.StateMutex.RLock()
    state := *w.State  // 创建状态的副本
    w.StateMutex.RUnlock()

    // 序列化状态
    stateData, err := json.Marshal(state)
    if err != nil {
        return fmt.Errorf("failed to marshal state: %v", err)
    }

    // 计算校验和
    checksum := calculateChecksum(stateData)

    // 创建状态文件名
    stateFile := fmt.Sprintf("states/%s/%d.state", w.ID, time.Now().UnixNano())

    // 写入HyDFS
    if err := w.HydfsClient.CreateFile(stateFile, stateData); err != nil {
        return fmt.Errorf("failed to persist state: %v", err)
    }

    // 记录状态持久化日志
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "STATE_PERSISTED",
        Message:   fmt.Sprintf("State persisted to %s", stateFile),
        Data: map[string]interface{}{
            "checksum": fmt.Sprintf("%x", checksum),
            "size":     len(stateData),
        },
    }

    return nil
}

func (w *Worker) loadState() error {
    // 获取最新的状态文件
    stateFiles, err := w.HydfsClient.ListFiles(fmt.Sprintf("states/%s", w.ID))
    if err != nil {
        return fmt.Errorf("failed to list state files: %v", err)
    }

    if len(stateFiles) == 0 {
        w.LogChan <- &LogEntry{
            Timestamp: time.Now(),
            Type:      "STATE_LOAD",
            Message:   "No previous state found",
        }
        return nil
    }

    // 获取最新的状态文件
    latestFile := stateFiles[len(stateFiles)-1]
    stateData, err := w.HydfsClient.ReadFile(latestFile)
    if err != nil {
        return fmt.Errorf("failed to read state file: %v", err)
    }

    // 反序列化状态
    var state State
    if err := json.Unmarshal(stateData, &state); err != nil {
        return fmt.Errorf("failed to unmarshal state: %v", err)
    }

    // 更新当前状态
    w.StateMutex.Lock()
    w.State = &state
    w.StateMutex.Unlock()

    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "STATE_LOADED",
        Message:   fmt.Sprintf("State loaded from %s", latestFile),
        Data: map[string]interface{}{
            "last_seq_num": state.LastSeqNum,
            "tuples_count": len(state.ProcessedTuples),
        },
    }

    return nil
}

// 状态恢复相关方法
func (w *Worker) recoverState() error {
    // 首先加载持久化的状态
    if err := w.loadState(); err != nil {
        return fmt.Errorf("failed to load state: %v", err)
    }

    // 重放操作日志以恢复最新状态
    if err := w.replayOperationLog(); err != nil {
        return fmt.Errorf("failed to replay operation log: %v", err)
    }

    // 验证状态一致性
    if err := w.validateState(); err != nil {
        return fmt.Errorf("state validation failed: %v", err)
    }

    return nil
}

func (w *Worker) replayOperationLog() error {
    // 获取上次检查点之后的操作日志
    logFiles, err := w.HydfsClient.ListFiles(fmt.Sprintf("logs/%s", w.ID))
    if err != nil {
        return fmt.Errorf("failed to list log files: %v", err)
    }

    w.StateMutex.RLock()
    lastSeqNum := w.State.LastSeqNum
    w.StateMutex.RUnlock()

    // 按时间顺序重放日志
    for _, logFile := range logFiles {
        logData, err := w.HydfsClient.ReadFile(logFile)
        if err != nil {
            continue
        }

        scanner := bufio.NewScanner(bytes.NewReader(logData))
        for scanner.Scan() {
            var entry LogEntry
            if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
                continue
            }

            // 只重放状态更新相关的日志
            if entry.Type == "STATE_UPDATE" {
                if err := w.replayStateUpdate(entry); err != nil {
                    w.logError("replay_failed", err)
                    continue
                }
            }
        }
    }

    // 记录重放完成
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "LOG_REPLAY_COMPLETED",
        Message:   "Operation log replay completed",
        Data: map[string]interface{}{
            "previous_seq": lastSeqNum,
            "current_seq":  w.State.LastSeqNum,
        },
    }

    return nil
}

func (w *Worker) replayStateUpdate(entry LogEntry) error {
    data, ok := entry.Data.(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid state update data format")
    }

    w.StateMutex.Lock()
    defer w.StateMutex.Unlock()

    switch entry.Type {
    case "STATE_UPDATE":
        if category, ok := data["category"].(string); ok {
            if count, ok := data["count"].(float64); ok {
                w.State.AggregateState[category] = int(count)
            }
        }
    }

    return nil
}

func (w *Worker) validateState() error {
    w.StateMutex.RLock()
    defer w.StateMutex.RUnlock()

    // 验证聚合状态的一致性
    if w.Role == "aggregate" {
        for category, count := range w.State.AggregateState {
            // 验证计数是否为非负数
            if count.(int) < 0 {
                return fmt.Errorf("invalid count for category %s: %d", category, count)
            }
        }
    }

    // 记录验证结果
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "STATE_VALIDATED",
        Message:   "State validation completed",
        Data: map[string]interface{}{
            "role":          w.Role,
            "aggregate_keys": len(w.State.AggregateState),
        },
    }

    return nil
}

// 快照创建
func (w *Worker) createSnapshot() error {
    w.StateMutex.RLock()
    stateSnapshot := *w.State
    w.StateMutex.RUnlock()

    snapshotData, err := json.Marshal(stateSnapshot)
    if err != nil {
        return fmt.Errorf("failed to marshal snapshot: %v", err)
    }

    snapshotFile := fmt.Sprintf("snapshots/%s/%d.snapshot", 
        w.ID, time.Now().UnixNano())

    if err := w.HydfsClient.CreateFile(snapshotFile, snapshotData); err != nil {
        return fmt.Errorf("failed to create snapshot: %v", err)
    }

    // 记录快照创建
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "SNAPSHOT_CREATED",
        Message:   fmt.Sprintf("Created snapshot: %s", snapshotFile),
        Data: map[string]interface{}{
            "size":          len(snapshotData),
            "processed_count": len(stateSnapshot.ProcessedTuples),
        },
    }

    return nil
}

// HyDFS 集成实现
type HydfsClient struct {
    Addr        string
    Port        int
    conn        net.Conn
    mutex       sync.Mutex
    retryCount  int
    timeout     time.Duration
}

// HyDFS操作请求
type HydfsRequest struct {
    Operation string      `json:"operation"`
    Path      string      `json:"path"`
    Data      []byte      `json:"data,omitempty"`
}

// HyDFS操作响应
type HydfsResponse struct {
    Success bool        `json:"success"`
    Data    []byte      `json:"data,omitempty"`
    Error   string      `json:"error,omitempty"`
}

func NewHydfsClient(addr string, port int) *HydfsClient {
    return &HydfsClient{
        Addr:       addr,
        Port:       port,
        retryCount: 3,
        timeout:    10 * time.Second,
    }
}

func (c *HydfsClient) connect() error {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    if c.conn != nil {
        return nil
    }

    var err error
    for i := 0; i < c.retryCount; i++ {
        c.conn, err = net.DialTimeout("tcp", 
            fmt.Sprintf("%s:%d", c.Addr, c.Port), 
            c.timeout)
        if err == nil {
            return nil
        }
        time.Sleep(time.Second * time.Duration(i+1))
    }
    return fmt.Errorf("failed to connect to HyDFS after %d attempts: %v", 
        c.retryCount, err)
}

func (c *HydfsClient) sendRequest(req *HydfsRequest) (*HydfsResponse, error) {
    if err := c.connect(); err != nil {
        return nil, err
    }

    c.mutex.Lock()
    defer c.mutex.Unlock()

    // 设置超时
    c.conn.SetDeadline(time.Now().Add(c.timeout))

    // 发送请求
    encoder := json.NewEncoder(c.conn)
    if err := encoder.Encode(req); err != nil {
        c.conn.Close()
        c.conn = nil
        return nil, fmt.Errorf("failed to send request: %v", err)
    }

    // 读取响应
    decoder := json.NewDecoder(c.conn)
    var resp HydfsResponse
    if err := decoder.Decode(&resp); err != nil {
        c.conn.Close()
        c.conn = nil
        return nil, fmt.Errorf("failed to read response: %v", err)
    }

    if !resp.Success {
        return nil, fmt.Errorf("HyDFS operation failed: %s", resp.Error)
    }

    return &resp, nil
}

func (c *HydfsClient) CreateFile(path string, data []byte) error {
    req := &HydfsRequest{
        Operation: "CREATE",
        Path:      path,
        Data:      data,
    }

    _, err := c.sendRequest(req)
    return err
}

func (c *HydfsClient) AppendFile(path string, data []byte) error {
    req := &HydfsRequest{
        Operation: "APPEND",
        Path:      path,
        Data:      data,
    }

    _, err := c.sendRequest(req)
    return err
}

func (c *HydfsClient) ReadFile(path string) ([]byte, error) {
    req := &HydfsRequest{
        Operation: "READ",
        Path:      path,
    }

    resp, err := c.sendRequest(req)
    if err != nil {
        return nil, err
    }

    return resp.Data, nil
}

func (c *HydfsClient) DeleteFile(path string) error {
    req := &HydfsRequest{
        Operation: "DELETE",
        Path:      path,
    }

    _, err := c.sendRequest(req)
    return err
}

func (c *HydfsClient) ListFiles(path string) ([]string, error) {
    req := &HydfsRequest{
        Operation: "LIST",
        Path:      path,
    }

    resp, err := c.sendRequest(req)
    if err != nil {
        return nil, err
    }

    var files []string
    if err := json.Unmarshal(resp.Data, &files); err != nil {
        return nil, fmt.Errorf("failed to parse file list: %v", err)
    }

    return files, nil
}

// 批处理操作
func (c *HydfsClient) BatchOperation(requests []*HydfsRequest) error {
    for _, req := range requests {
        if _, err := c.sendRequest(req); err != nil {
            return fmt.Errorf("batch operation failed at %s: %v", 
                req.Path, err)
        }
    }
    return nil
}

// 文件块操作
func (c *HydfsClient) WriteBlock(path string, blockID int, data []byte) error {
    blockPath := fmt.Sprintf("%s.block%d", path, blockID)
    return c.CreateFile(blockPath, data)
}

func (c *HydfsClient) ReadBlock(path string, blockID int) ([]byte, error) {
    blockPath := fmt.Sprintf("%s.block%d", path, blockID)
    return c.ReadFile(blockPath)
}

// 辅助方法
func (c *HydfsClient) ensureDirectory(path string) error {
    parts := strings.Split(path, "/")
    currentPath := ""
    
    for _, part := range parts[:len(parts)-1] {
        currentPath = filepath.Join(currentPath, part)
        req := &HydfsRequest{
            Operation: "MKDIR",
            Path:      currentPath,
        }
        
        if _, err := c.sendRequest(req); err != nil {
            return fmt.Errorf("failed to create directory %s: %v", 
                currentPath, err)
        }
    }
    
    return nil
}

// 实用函数
func (c *HydfsClient) fileExists(path string) (bool, error) {
    _, err := c.ReadFile(path)
    if err == nil {
        return true, nil
    }
    
    if strings.Contains(err.Error(), "not found") {
        return false, nil
    }
    
    return false, err
}

// 错误处理和重试
func (c *HydfsClient) withRetry(op func() error) error {
    var lastErr error
    for i := 0; i < c.retryCount; i++ {
        if err := op(); err == nil {
            return nil
        } else {
            lastErr = err
            // 重置连接
            c.mutex.Lock()
            if c.conn != nil {
                c.conn.Close()
                c.conn = nil
            }
            c.mutex.Unlock()
            time.Sleep(time.Second * time.Duration(i+1))
        }
    }
    return fmt.Errorf("operation failed after %d retries: %v", 
        c.retryCount, lastErr)
}

func (c *HydfsClient) Close() error {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    if c.conn != nil {
        err := c.conn.Close()
        c.conn = nil
        return err
    }
    return nil
}

// 网络通信相关实现

// 消息类型定义补充
const (
    MsgStateUpdate   MessageType = "STATE_UPDATE"
    MsgTaskAssign    MessageType = "TASK_ASSIGN"
    MsgTaskComplete  MessageType = "TASK_COMPLETE"
    MsgWorkerFailed  MessageType = "WORKER_FAILED"
)

// 网络配置
type NetworkConfig struct {
    ListenAddr string
    Port      int
    Timeout   time.Duration
}

// Worker 网络相关方法
func (w *Worker) startNetwork() error {
    addr := fmt.Sprintf("%s:%d", w.Config.Addr, w.Config.Port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        return fmt.Errorf("failed to start listener: %v", err)
    }
    w.Listener = listener

    // 启动消息处理goroutine
    go w.handleConnections()
    go w.maintainConnections()

    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "NETWORK_STARTED",
        Message:   fmt.Sprintf("Started network listener on %s", addr),
    }

    return nil
}

func (w *Worker) handleConnections() {
    for {
        conn, err := w.Listener.Accept()
        if err != nil {
            select {
            case <-w.StopChan:
                return
            default:
                w.logError("accept_error", err)
                continue
            }
        }
        go w.handleConnection(conn)
    }
}

func (w *Worker) handleConnection(conn net.Conn) {
    defer conn.Close()

    // 设置连接超时
    conn.SetDeadline(time.Now().Add(ProcessTimeout))

    // 解码消息
    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        w.logError("decode_error", err)
        return
    }

    // 处理消息
    response, err := w.processMessage(msg)
    if err != nil {
        response = Message{
            Type:      MsgError,
            SenderID:  w.ID,
            Timestamp: time.Now(),
            Data:      err.Error(),
        }
    }

    // 发送响应
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        w.logError("encode_error", err)
    }
}

func (w *Worker) processMessage(msg Message) (Message, error) {
    w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Type:      "MESSAGE_RECEIVED",
        Message:   fmt.Sprintf("Received message type %s from %s", msg.Type, msg.SenderID),
    }

    switch msg.Type {
    case MsgTuple:
        return w.handleTupleMessage(msg)
    case MsgBatch:
        return w.handleBatchMessage(msg)
    case MsgStateUpdate:
        return w.handleStateUpdateMessage(msg)
    case MsgHeartbeat:
        return w.handleHeartbeatMessage(msg)
    default:
        return Message{}, fmt.Errorf("unknown message type: %s", msg.Type)
    }
}

func (w *Worker) handleTupleMessage(msg Message) (Message, error) {
    tuple, ok := msg.Data.(*Tuple)
    if !ok {
        return Message{}, fmt.Errorf("invalid tuple data format")
    }

    // 将tuple发送到处理通道
    select {
    case w.InputChan <- tuple:
        return Message{
            Type:      MsgAck,
            SenderID:  w.ID,
            Timestamp: time.Now(),
            Data:      tuple.ID,
        }, nil
    case <-time.After(ProcessTimeout):
        return Message{}, ErrTimeout
    }
}

func (w *Worker) handleBatchMessage(msg Message) (Message, error) {
    batch, ok := msg.Data.(*Batch)
    if !ok {
        return Message{}, fmt.Errorf("invalid batch data format")
    }

    // 验证批次校验和
    batchData, err := json.Marshal(batch.Tuples)
    if err != nil {
        return Message{}, err
    }

    checksum := calculateChecksum(batchData)
    if !bytes.Equal(checksum, batch.Checksum) {
        return Message{}, fmt.Errorf("batch checksum mismatch")
    }

    // 处理批次中的每个tuple
    for _, tuple := range batch.Tuples {
        select {
        case w.InputChan <- tuple:
        case <-time.After(ProcessTimeout):
            return Message{}, ErrTimeout
        }
    }

    return Message{
        Type:      MsgAck,
        SenderID:  w.ID,
        Timestamp: time.Now(),
        Data:      batch.ID,
    }, nil
}

func (w *Worker) handleStateUpdateMessage(msg Message) (Message, error) {
    var state State
    data, err := json.Marshal(msg.Data)
    if err != nil {
        return Message{}, err
    }

    if err := json.Unmarshal(data, &state); err != nil {
        return Message{}, err
    }

    // 合并状态
    w.StateMutex.Lock()
    w.mergeState(&state)
    w.StateMutex.Unlock()

    return Message{
        Type:      MsgAck,
        SenderID:  w.ID,
        Timestamp: time.Now(),
    }, nil
}

func (w *Worker) handleHeartbeatMessage(msg Message) (Message, error) {
    // 更新发送者的最后心跳时间
    w.memberMutex.Lock()
    if member, exists := w.members[msg.SenderID]; exists {
        member.LastHeartbeat = msg.Timestamp
        member.Status = StatusNormal
    }
    w.memberMutex.Unlock()

    return Message{
        Type:      MsgAck,
        SenderID:  w.ID,
        Timestamp: time.Now(),
    }, nil
}

func (w *Worker) sendMessage(targetID string, msg Message) error {
    w.memberMutex.RLock()
    target, exists := w.members[targetID]
    w.memberMutex.RUnlock()

    if !exists {
        return ErrNodeNotFound
    }

    // 获取或创建连接
    conn, err := w.getConnection(targetID, target)
    if err != nil {
        return err
    }

    // 发送消息
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        w.closeConnection(targetID)
        return err
    }

    // 读取响应
    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        w.closeConnection(targetID)
        return err
    }

    if response.Type == MsgError {
        return fmt.Errorf("remote error: %v", response.Data)
    }

    return nil
}

func (w *Worker) getConnection(targetID string, target *MemberInfo) (net.Conn, error) {
    w.ConnMutex.RLock()
    conn, exists := w.Connections[targetID]
    w.ConnMutex.RUnlock()

    if exists {
        return conn, nil
    }

    // 创建新连接
    w.ConnMutex.Lock()
    defer w.ConnMutex.Unlock()

    // 双重检查
    if conn, exists = w.Connections[targetID]; exists {
        return conn, nil
    }

    conn, err := net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", target.Address, target.Port), 
        ProcessTimeout)
    if err != nil {
        return nil, err
    }

    w.Connections[targetID] = conn
    return conn, nil
}

func (w *Worker) closeConnection(targetID string) {
    w.ConnMutex.Lock()
    defer w.ConnMutex.Unlock()

    if conn, exists := w.Connections[targetID]; exists {
        conn.Close()
        delete(w.Connections, targetID)
    }
}

func (w *Worker) maintainConnections() {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-w.StopChan:
            return
        case <-ticker.C:
            w.cleanupConnections()
        }
    }
}

func (w *Worker) cleanupConnections() {
    w.ConnMutex.Lock()
    defer w.ConnMutex.Unlock()

    for id, conn := range w.Connections {
        // 测试连接是否有效
        if err := w.testConnection(conn); err != nil {
            conn.Close()
            delete(w.Connections, id)
            w.LogChan <- &LogEntry{
                Timestamp: time.Now(),
                Type:      "CONNECTION_CLOSED",
                Message:   fmt.Sprintf("Closed stale connection to %s", id),
            }
        }
    }
}

func (w *Worker) testConnection(conn net.Conn) error {
    // 设置写入超时
    conn.SetWriteDeadline(time.Now().Add(time.Second))
    
    // 尝试写入一个字节
    _, err := conn.Write([]byte{0})
    return err
}

// 主程序、测试和工具函数实现

// 命令行选项
type CommandLineOptions struct {
    NodeID      string
    Role        string
    SourceFile  string
    DestFile    string
    NumTasks    int
    ParamX      string
    HydfsAddr   string
    HydfsPort   int
    LogDir      string
}

func parseCommandLine() *CommandLineOptions {
    opts := &CommandLineOptions{}
    
    flag.StringVar(&opts.NodeID, "id", "", "Node ID")
    flag.StringVar(&opts.Role, "role", "", "Node role (leader/worker)")
    flag.StringVar(&opts.SourceFile, "src", "", "Source file path")
    flag.StringVar(&opts.DestFile, "dest", "", "Destination file path")
    flag.IntVar(&opts.NumTasks, "tasks", 3, "Number of tasks")
    flag.StringVar(&opts.ParamX, "param-x", "", "Parameter X value")
    flag.StringVar(&opts.HydfsAddr, "hydfs-addr", "localhost", "HyDFS server address")
    flag.IntVar(&opts.HydfsPort, "hydfs-port", 5000, "HyDFS server port")
    flag.StringVar(&opts.LogDir, "log-dir", "logs", "Log directory")
    
    flag.Parse()
    
    return opts
}

func main() {
    // 解析命令行参数
    opts := parseCommandLine()

    // 验证必要参数
    if opts.NodeID == "" {
        log.Fatal("Node ID is required")
    }
    if opts.Role == "" {
        log.Fatal("Role is required")
    }

    // 设置日志
    if err := setupLogging(opts.NodeID, opts.LogDir); err != nil {
        log.Fatalf("Failed to setup logging: %v", err)
    }

    // 创建配置
    config := WorkerConfig{
        ID:         opts.NodeID,
        Role:       opts.Role,
        BatchSize:  DefaultBatchSize,
        ParamX:     opts.ParamX,
        HydfsAddr:  opts.HydfsAddr,
        HydfsPort:  opts.HydfsPort,
        LogDir:     opts.LogDir,
    }

    // 根据角色启动不同类型的节点
    switch opts.Role {
    case "leader":
        if err := runLeader(config, opts); err != nil {
            log.Fatalf("Leader failed: %v", err)
        }
    case "worker":
        if err := runWorker(config); err != nil {
            log.Fatalf("Worker failed: %v", err)
        }
    default:
        log.Fatalf("Unknown role: %s", opts.Role)
    }
}

func runLeader(config WorkerConfig, opts *CommandLineOptions) error {
    // 验证leader特定参数
    if opts.SourceFile == "" || opts.DestFile == "" {
        return fmt.Errorf("source and destination files are required for leader")
    }

    // 创建leader
    leader := NewLeader(config.ID, LeaderConfig{
        NumTasks:   opts.NumTasks,
        SourceFile: opts.SourceFile,
        DestFile:   opts.DestFile,
        ParamX:     opts.ParamX,
    })

    // 启动leader
    if err := leader.Start(); err != nil {
        return fmt.Errorf("failed to start leader: %v", err)
    }

    // 等待信号
    waitForSignal(leader)
    return nil
}

func runWorker(config WorkerConfig) error {
    // 创建worker
    worker, err := NewWorker(config)
    if err != nil {
        return fmt.Errorf("failed to create worker: %v", err)
    }

    // 启动worker
    if err := worker.Start(); err != nil {
        return fmt.Errorf("failed to start worker: %v", err)
    }

    // 等待信号
    waitForSignal(worker)
    return nil
}

// 日志设置
func setupLogging(nodeID, logDir string) error {
    // 创建日志目录
    if err := os.MkdirAll(logDir, 0755); err != nil {
        return fmt.Errorf("failed to create log directory: %v", err)
    }

    // 设置日志文件
    logFile := filepath.Join(logDir, fmt.Sprintf("%s.log", nodeID))
    file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return fmt.Errorf("failed to open log file: %v", err)
    }

    // 设置日志输出到文件和标准输出
    log.SetOutput(io.MultiWriter(os.Stdout, file))
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    log.SetPrefix(fmt.Sprintf("[%s] ", nodeID))

    return nil
}

// 测试用例
func TestBasicWordCount(t *testing.T) {
    // 创建测试配置
    config := WorkerConfig{
        ID:         "test-worker",
        Role:       "worker",
        BatchSize:  10,
        ParamX:     "Stop",
        LogDir:     "test-logs",
    }

    // 创建worker
    worker, err := NewWorker(config)
    if err != nil {
        t.Fatalf("Failed to create worker: %v", err)
    }

    // 启动worker
    if err := worker.Start(); err != nil {
        t.Fatalf("Failed to start worker: %v", err)
    }
    defer worker.Stop()

    // 准备测试数据
    testData := []string{
        "1,Test St,North,Stop,Warning",
        "2,Main St,South,Stop,Caution",
        "3,Oak St,East,Yield,Warning",
    }

    // 发送测试数据
    for _, line := range testData {
        tuple := &Tuple{
            ID:        fmt.Sprintf("test-%d", time.Now().UnixNano()),
            Key:       line,
            Value:     line,
            Timestamp: time.Now(),
            BatchID:   "test-batch",
            Source:    "test",
        }
        worker.InputChan <- tuple
    }

    // 等待处理完成
    time.Sleep(time.Second)

    // 验证结果
    worker.StateMutex.RLock()
    counts := worker.State.AggregateState
    worker.StateMutex.RUnlock()

    if count, ok := counts["Warning"].(int); !ok || count != 2 {
        t.Errorf("Expected Warning count to be 2, got %v", counts["Warning"])
    }
}

func TestFailureRecovery(t *testing.T) {
    // 创建leader配置
    leaderConfig := LeaderConfig{
        NumTasks:   3,
        SourceFile: "test_data.csv",
        DestFile:   "test_output.csv",
        ParamX:     "Stop",
    }

    // 创建leader
    leader := NewLeader("test-leader", leaderConfig)
    if err := leader.Start(); err != nil {
        t.Fatalf("Failed to start leader: %v", err)
    }
    defer leader.Stop()

    // 创建workers
    workers := make([]*Worker, 3)
    for i := 0; i < 3; i++ {
        config := WorkerConfig{
            ID:       fmt.Sprintf("worker-%d", i),
            Role:     "worker",
            ParamX:   "Stop",
        }
        worker, err := NewWorker(config)
        if err != nil {
            t.Fatalf("Failed to create worker %d: %v", i, err)
        }
        if err := worker.Start(); err != nil {
            t.Fatalf("Failed to start worker %d: %v", i, err)
        }
        workers[i] = worker
        defer worker.Stop()
    }

    // 模拟worker故障
    workers[1].Stop()
    time.Sleep(2 * time.Second)

    // 验证任务重新分配
    remainingWorkers := len(leader.GetActiveWorkers())
    if remainingWorkers != 2 {
        t.Errorf("Expected 2 active workers, got %d", remainingWorkers)
    }

    // 验证状态恢复
    // TODO: Add more specific state recovery tests
}

func TestExactlyOnceProcessing(t *testing.T) {
    config := WorkerConfig{
        ID:         "test-worker",
        Role:       "worker",
        BatchSize:  10,
        ParamX:     "Stop",
    }

    worker, err := NewWorker(config)
    if err != nil {
        t.Fatalf("Failed to create worker: %v", err)
    }
    if err := worker.Start(); err != nil {
        t.Fatalf("Failed to start worker: %v", err)
    }
    defer worker.Stop()

    // 发送重复的tuple
    tuple := &Tuple{
        ID:        "duplicate-id",
        Key:       "test",
        Value:     "test data",
        Timestamp: time.Now(),
    }

    // 发送同一个tuple两次
    worker.InputChan <- tuple
    worker.InputChan <- tuple

    time.Sleep(time.Second)

    // 验证只处理了一次
    worker.StateMutex.RLock()
    processCount := len(worker.State.ProcessedTuples)
    worker.StateMutex.RUnlock()

    if processCount != 1 {
        t.Errorf("Expected 1 processed tuple, got %d", processCount)
    }
}

// 实用函数
func waitForSignal(node interface{}) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    switch n := node.(type) {
    case *Leader:
        n.Stop()
    case *Worker:
        n.Stop()
    }
}

// 命令行验证
func validateCommandLine(opts *CommandLineOptions) error {
    if opts.NodeID == "" {
        return fmt.Errorf("node ID is required")
    }
    if opts.Role == "" {
        return fmt.Errorf("role is required")
    }
    if opts.Role == "leader" {
        if opts.SourceFile == "" || opts.DestFile == "" {
            return fmt.Errorf("source and destination files are required for leader")
        }
        if opts.NumTasks < 1 {
            return fmt.Errorf("number of tasks must be positive")
        }
    }
    return nil
}

// CSV处理工具函数
func parseCSVLine(line string) ([]string, error) {
    r := csv.NewReader(strings.NewReader(line))
    r.FieldsPerRecord = -1 // 允许变长记录
    fields, err := r.Read()
    if err != nil {
        return nil, fmt.Errorf("failed to parse CSV line: %v", err)
    }
    return fields, nil
}

func formatCSVLine(fields []string) string {
    var buf bytes.Buffer
    w := csv.NewWriter(&buf)
    w.Write(fields)
    w.Flush()
    return strings.TrimSpace(buf.String())
}

// 校验和计算
func calculateChecksum(data []byte) []byte {
    hash := sha256.New()
    hash.Write(data)
    return hash.Sum(nil)
}
