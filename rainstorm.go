package main

import (
    "bytes"
    "hash/fnv"
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

type CommandLineOptions struct {
    NodeID      string
    Role        string
    Addr        string
    Port        int
    SourceFile  string
    DestFile    string
    NumTasks    int
    ParamX      string
    HydfsAddr   string
    HydfsPort   int
    LogDir      string
}

// 错误定义
var (
    ErrFileNotFound     = errors.New("file not found")
    ErrDuplicateTuple   = errors.New("duplicate tuple")
    ErrNodeNotFound     = errors.New("node not found")
    ErrInvalidOperation = errors.New("invalid operation")
    ErrTimeout         = errors.New("operation timed out")
)

// NodeStatus 定义
type NodeStatus int

const (
    StatusNormal NodeStatus = iota
    StatusSuspected
    StatusFailed
)

// MessageType 定义
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

// MemberInfo 结构体
type MemberInfo struct {
    ID            string     `json:"id"`
    Address       string     `json:"address"`
    Port          int        `json:"port"`
    Status        NodeStatus `json:"status"`
    LastHeartbeat time.Time  `json:"last_heartbeat"`
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
    Role        string
    Addr        string
    Port        int
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
    
    StateMutex     sync.RWMutex
    BatchMutex     sync.RWMutex
    
    Listener       net.Listener
    Connections    map[string]net.Conn
    ConnMutex      sync.RWMutex
    
    HydfsClient    *HydfsClient
    
    Logger         *log.Logger
    LogFile        *os.File

    members        map[string]*MemberInfo
    memberMutex    sync.RWMutex
}

// HydfsClient 定义
type HydfsClient struct {
    Addr     string
    Port     int
    conn     net.Conn
    mutex    sync.Mutex
}

// 辅助函数
func calculateChecksum(data []byte) []byte {
    hash := sha256.New()
    hash.Write(data)
    return hash.Sum(nil)
}

// HydfsClient 方法实现
func NewHydfsClient(addr string, port int) (*HydfsClient, error) {
    return &HydfsClient{
        Addr: addr,
        Port: port,
    }, nil
}

func (c *HydfsClient) connect() error {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    if c.conn != nil {
        return nil
    }

    var err error
    c.conn, err = net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", c.Addr, c.Port), 
        ProcessTimeout)
    if err != nil {
        return err
    }
    return nil
}

func (c *HydfsClient) CreateFile(path string, data []byte) error {
    if err := c.connect(); err != nil {
        return err
    }

    req := struct {
        Op   string `json:"op"`
        Path string `json:"path"`
        Data []byte `json:"data"`
    }{
        Op:   "CREATE",
        Path: path,
        Data: data,
    }

    return c.sendRequest(req)
}

func (c *HydfsClient) AppendFile(path string, data []byte) error {
    if err := c.connect(); err != nil {
        return err
    }

    req := struct {
        Op   string `json:"op"`
        Path string `json:"path"`
        Data []byte `json:"data"`
    }{
        Op:   "APPEND",
        Path: path,
        Data: data,
    }

    return c.sendRequest(req)
}

func (c *HydfsClient) ReadFile(path string) ([]byte, error) {
    if err := c.connect(); err != nil {
        return nil, err
    }

    req := struct {
        Op   string `json:"op"`
        Path string `json:"path"`
    }{
        Op:   "READ",
        Path: path,
    }

    resp, err := c.sendRequestWithResponse(req)
    if err != nil {
        return nil, err
    }

    return resp.Data, nil
}

func (c *HydfsClient) ListFiles(prefix string) ([]string, error) {
    if err := c.connect(); err != nil {
        return nil, err
    }

    req := struct {
        Op     string `json:"op"`
        Prefix string `json:"prefix"`
    }{
        Op:     "LIST",
        Prefix: prefix,
    }

    resp, err := c.sendRequestWithResponse(req)
    if err != nil {
        return nil, err
    }

    var files []string
    if err := json.Unmarshal(resp.Data, &files); err != nil {
        return nil, err
    }

    return files, nil
}

func (c *HydfsClient) sendRequest(req interface{}) error {
    resp, err := c.sendRequestWithResponse(req)
    if err != nil {
        return err
    }
    return nil
}

func (c *HydfsClient) sendRequestWithResponse(req interface{}) (*struct {
    Success bool   `json:"success"`
    Data    []byte `json:"data"`
    Error   string `json:"error"`
}, error) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    encoder := json.NewEncoder(c.conn)
    if err := encoder.Encode(req); err != nil {
        return nil, err
    }

    var resp struct {
        Success bool   `json:"success"`
        Data    []byte `json:"data"`
        Error   string `json:"error"`
    }

    decoder := json.NewDecoder(c.conn)
    if err := decoder.Decode(&resp); err != nil {
        return nil, err
    }

    if !resp.Success {
        return nil, errors.New(resp.Error)
    }

    return &resp, nil
}

// Worker 方法实现
func NewWorker(config WorkerConfig) (*Worker, error) {
    w := &Worker{
        Config:      config,
        ID:          config.ID,
        Role:        config.Role,
        State: &State{
            ProcessedTuples: make(map[string]bool),
            AggregateState:  make(map[string]interface{}),
            LastSeqNum:      0,
        },
        InputChan:    make(chan *Tuple, DefaultBatchSize),
        OutputChan:   make(chan *Tuple, DefaultBatchSize),
        LogChan:      make(chan *LogEntry, LogBufferSize),
        StopChan:     make(chan struct{}),
        Connections:  make(map[string]net.Conn),
        members:      make(map[string]*MemberInfo),
    }

    // 初始化HydfsClient
    hydfsClient, err := NewHydfsClient(config.HydfsAddr, config.HydfsPort)
    if err != nil {
        return nil, err
    }
    w.HydfsClient = hydfsClient

    // 初始化日志
    if err := w.initLogger(); err != nil {
        return nil, err
    }

    return w, nil
}

func (w *Worker) initLogger() error {
    // 创建日志目录
    if err := os.MkdirAll(w.Config.LogDir, 0755); err != nil {
        return fmt.Errorf("failed to create log directory: %v", err)
    }

    // 打开日志文件
    logFile := filepath.Join(w.Config.LogDir, fmt.Sprintf("%s.log", w.ID))
    file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return fmt.Errorf("failed to open log file: %v", err)
    }
    w.LogFile = file

    // 设置日志
    w.Logger = log.New(io.MultiWriter(os.Stdout, file), 
        fmt.Sprintf("[%s] ", w.ID), 
        log.LstdFlags|log.Lmicroseconds)

    return nil
}

// Worker 核心处理逻辑
func (w *Worker) Start() error {
    // 启动网络监听
    if err := w.startNetwork(); err != nil {
        return err
    }

    // 启动处理goroutines
    go w.processLoop()
    go w.logLoop()
    go w.handleConnections()
    go w.stateSync()

    w.Logger.Printf("Worker started [Role: %s]", w.Role)
    return nil
}

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

func (w *Worker) processTuple(tuple *Tuple) error {
    // 检查重复
    tupleID := fmt.Sprintf("%s-%s", tuple.Source, tuple.ID)
    w.StateMutex.RLock()
    if w.State.ProcessedTuples[tupleID] {
        w.StateMutex.RUnlock()
        w.logEvent("DUPLICATE_DETECTED", fmt.Sprintf("Duplicate tuple: %s", tupleID), nil)
        return nil
    }
    w.StateMutex.RUnlock()

    var result []*Tuple
    var err error

    // 根据角色处理tuple
    switch w.Role {
    case "source":
        result, err = w.handleSourceTuple(tuple)
    case "transform":
        result, err = w.handleTransformTuple(tuple)
    case "aggregate":
        result, err = w.handleAggregateTuple(tuple)
    default:
        return fmt.Errorf("unknown role: %s", w.Role)
    }

    if err != nil {
        return err
    }

    // 记录处理状态
    w.StateMutex.Lock()
    w.State.ProcessedTuples[tupleID] = true
    w.State.LastSeqNum++
    w.StateMutex.Unlock()

    // 将结果加入批次
    return w.addToBatch(result)
}

func (w *Worker) handleTupleMessage(msg Message) error {
    tuple, ok := msg.Data.(*Tuple)
    if !ok {
        return fmt.Errorf("invalid tuple data format")
    }

    select {
    case w.InputChan <- tuple:
        return nil
    case <-time.After(ProcessTimeout):
        return ErrTimeout
    }
}

func (w *Worker) handleBatchMessage(msg Message) error {
    batch, ok := msg.Data.(*Batch)
    if !ok {
        return fmt.Errorf("invalid batch data format")
    }

    // 验证批次校验和
    batchData, err := json.Marshal(batch.Tuples)
    if err != nil {
        return err
    }

    if !bytes.Equal(calculateChecksum(batchData), batch.Checksum) {
        return fmt.Errorf("batch checksum mismatch")
    }

    for _, tuple := range batch.Tuples {
        if err := w.processTuple(tuple); err != nil {
            return err
        }
    }

    return nil
}

func (w *Worker) handleStateSyncMessage(msg Message) error {
    var state State
    data, err := json.Marshal(msg.Data)
    if err != nil {
        return err
    }

    if err := json.Unmarshal(data, &state); err != nil {
        return err
    }

    // 合并状态
    w.StateMutex.Lock()
    defer w.StateMutex.Unlock()

    // 更新状态
    for id := range state.ProcessedTuples {
        w.State.ProcessedTuples[id] = true
    }

    if w.Role == "aggregate" {
        for k, v := range state.AggregateState {
            if current, exists := w.State.AggregateState[k]; exists {
                w.State.AggregateState[k] = current.(int) + v.(int)
            } else {
                w.State.AggregateState[k] = v
            }
        }
    }

    return nil
}

func (w *Worker) sendMessage(targetID string, msg Message) error {
    w.memberMutex.RLock()
    target, exists := w.members[targetID]
    w.memberMutex.RUnlock()

    if !exists {
        return ErrNodeNotFound
    }

    addr := fmt.Sprintf("%s:%d", target.Address, target.Port)
    conn, err := net.DialTimeout("tcp", addr, ProcessTimeout)
    if err != nil {
        return fmt.Errorf("failed to connect to %s: %v", targetID, err)
    }
    defer conn.Close()

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to send message: %v", err)
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return fmt.Errorf("failed to read response: %v", err)
    }

    if response.Type == MsgError {
        return fmt.Errorf("remote error: %v", response.Data)
    }

    return nil
}

func (w *Worker) handleHeartbeatMessage(msg Message) error {
    w.memberMutex.Lock()
    defer w.memberMutex.Unlock()

    if member, exists := w.members[msg.SenderID]; exists {
        member.LastHeartbeat = msg.Timestamp
        member.Status = StatusNormal
    }

    return nil
}

func (w *Worker) handleSourceTuple(tuple *Tuple) ([]*Tuple, error) {
    line, ok := tuple.Value.(string)
    if !ok {
        return nil, fmt.Errorf("invalid source tuple value type")
    }

    // 创建新tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       fmt.Sprintf("line-%d", w.State.LastSeqNum),
        Value:     line,
        Timestamp: time.Now(),
        BatchID:   fmt.Sprintf("batch-%s-%d", w.ID, int(w.State.LastSeqNum/int64(w.Config.BatchSize))),
        Source:    w.ID,
    }

    w.logEvent("SOURCE_PROCESSED", fmt.Sprintf("Processed source tuple: %s", newTuple.ID), nil)
    return []*Tuple{newTuple}, nil
}

func (w *Worker) handleTransformTuple(tuple *Tuple) ([]*Tuple, error) {
    line, ok := tuple.Value.(string)
    if !ok {
        return nil, fmt.Errorf("invalid transform tuple value type")
    }

    // 解析CSV行
    reader := csv.NewReader(strings.NewReader(line))
    fields, err := reader.Read()
    if err != nil {
        return nil, err
    }

    // 检查Pattern X
    if !strings.Contains(line, w.Config.ParamX) {
        return nil, nil
    }

    // 提取所需字段
    if len(fields) < 4 {
        return nil, fmt.Errorf("insufficient fields in CSV")
    }

    objectID := fields[0]
    signType := fields[3]

    // 创建结果tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       objectID,
        Value:     fmt.Sprintf("%s,%s", objectID, signType),
        Timestamp: time.Now(),
        BatchID:   tuple.BatchID,
        Source:    w.ID,
    }

    w.logEvent("TRANSFORM_PROCESSED", fmt.Sprintf("Processed transform tuple: %s", newTuple.ID), nil)
    return []*Tuple{newTuple}, nil
}

func (w *Worker) handleAggregateTuple(tuple *Tuple) ([]*Tuple, error) {
    line, ok := tuple.Value.(string)
    if !ok {
        return nil, fmt.Errorf("invalid aggregate tuple value type")
    }

    // 解析CSV行
    reader := csv.NewReader(strings.NewReader(line))
    fields, err := reader.Read()
    if err != nil {
        return nil, err
    }

    // 检查字段数量
    if len(fields) < 5 {
        return nil, fmt.Errorf("insufficient fields")
    }

    // 检查Sign Post类型
    signPost := fields[3]
    if signPost != w.Config.ParamX {
        return nil, nil
    }

    // 获取Category
    category := fields[4]
    if category == "" {
        category = "empty"
    } else if category == " " {
        category = "space"
    }

    // 更新计数
    w.StateMutex.Lock()
    if _, exists := w.State.AggregateState[category]; !exists {
        w.State.AggregateState[category] = 0
    }
    count := w.State.AggregateState[category].(int) + 1
    w.State.AggregateState[category] = count
    w.StateMutex.Unlock()

    // 创建结果tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.State.LastSeqNum),
        Key:       category,
        Value:     count,
        Timestamp: time.Now(),
        BatchID:   tuple.BatchID,
        Source:    w.ID,
    }

    w.logEvent("AGGREGATE_PROCESSED", 
        fmt.Sprintf("Updated count for category %s: %d", category, count),
        map[string]interface{}{
            "category": category,
            "count":   count,
        })

    return []*Tuple{newTuple}, nil
}

// 批处理和网络通信实现
func (w *Worker) addToBatch(tuples []*Tuple) error {
    w.BatchMutex.Lock()
    defer w.BatchMutex.Unlock()

    if w.CurrentBatch == nil {
        w.CurrentBatch = &Batch{
            ID:            fmt.Sprintf("batch-%s-%d", w.ID, int(w.State.LastSeqNum/int64(w.Config.BatchSize))),
            Tuples:        make([]*Tuple, 0),
            ProcessedIDs:  make(map[string]bool),
            NextStageAcks: make(map[string]bool),
            CreateTime:    time.Now(),
        }
    }

    // 添加tuples到当前批次
    for _, tuple := range tuples {
        w.CurrentBatch.Tuples = append(w.CurrentBatch.Tuples, tuple)
    }

    // 如果批次已满，发送它
    if len(w.CurrentBatch.Tuples) >= w.Config.BatchSize {
        return w.sendBatch(w.CurrentBatch)
    }

    return nil
}

func (w *Worker) checkAndSendBatch() {
    w.BatchMutex.Lock()
    defer w.BatchMutex.Unlock()

    if w.CurrentBatch == nil || len(w.CurrentBatch.Tuples) == 0 {
        return
    }

    if time.Since(w.CurrentBatch.CreateTime) >= ProcessTimeout {
        if err := w.sendBatch(w.CurrentBatch); err != nil {
            w.logError("send_batch_failed", err)
        }
    }
}

func (w *Worker) sendBatch(batch *Batch) error {
    // 计算批次校验和
    data, err := json.Marshal(batch.Tuples)
    if err != nil {
        return err
    }
    batch.Checksum = calculateChecksum(data)

    // 记录批次
    w.logEvent("BATCH_SENDING", 
        fmt.Sprintf("Sending batch %s with %d tuples", batch.ID, len(batch.Tuples)),
        map[string]interface{}{
            "batch_id":     batch.ID,
            "tuple_count":  len(batch.Tuples),
            "checksum":     fmt.Sprintf("%x", batch.Checksum),
        })

    // 发送到下一阶段
    select {
    case w.OutputChan <- &Tuple{
        ID:        batch.ID,
        Value:     batch,
        Timestamp: time.Now(),
        Source:    w.ID,
        BatchID:   batch.ID,
        Checksum:  batch.Checksum,
    }:
        w.CurrentBatch = nil
        return nil
    case <-time.After(ProcessTimeout):
        return ErrTimeout
    }
}

// 网络通信相关
func (w *Worker) startNetwork() error {
    addr := fmt.Sprintf("%s:%d", w.Config.Addr, w.Config.Port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        return fmt.Errorf("failed to start listener: %v", err)
    }
    w.Listener = listener

    w.logEvent("NETWORK_STARTED", 
        fmt.Sprintf("Started network listener on %s", addr),
        nil)

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

    conn.SetDeadline(time.Now().Add(ProcessTimeout))

    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        w.logError("decode_error", err)
        return
    }

    response := w.processMessage(msg)  

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        w.logError("encode_error", err)
    }
}

func (w *Worker) processMessage(msg Message) Message {  // 明确指定返回类型为Message
    w.logEvent("MESSAGE_RECEIVED", 
        fmt.Sprintf("Received message type %s from %s", msg.Type, msg.SenderID),
        nil)

    var response Message
    response.Type = MsgAck
    response.SenderID = w.ID
    response.Timestamp = time.Now()

    var err error
    switch msg.Type {
    case MsgTuple:
        err = w.handleTupleMessage(msg)
    case MsgBatch:
        err = w.handleBatchMessage(msg)
    case MsgStateSync:
        err = w.handleStateSyncMessage(msg)
    case MsgHeartbeat:
        err = w.handleHeartbeatMessage(msg)
    default:
        err = fmt.Errorf("unknown message type: %s", msg.Type)
    }

    if err != nil {
        return Message{
            Type:      MsgError,
            SenderID:  w.ID,
            Timestamp: time.Now(),
            Data:      err.Error(),
        }
    }

    return response
}

// 状态同步和故障恢复实现
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

func (w *Worker) persistState() error {
    w.StateMutex.RLock()
    state := *w.State // 创建状态副本
    w.StateMutex.RUnlock()

    // 序列化状态
    stateData, err := json.Marshal(state)
    if err != nil {
        return fmt.Errorf("failed to marshal state: %v", err)
    }

    // 计算校验和
    stateChecksum := calculateChecksum(stateData)

    // 创建状态文件名
    stateFile := fmt.Sprintf("states/%s/%d.state", w.ID, time.Now().UnixNano())

    // 确保目录存在
    if err := w.HydfsClient.ensureDirectory("states/" + w.ID); err != nil {
        return err
    }

    // 写入HyDFS
    if err := w.HydfsClient.CreateFile(stateFile, stateData); err != nil {
        return fmt.Errorf("failed to persist state: %v", err)
    }

    w.logEvent("STATE_PERSISTED", 
        fmt.Sprintf("State persisted to %s", stateFile),
        map[string]interface{}{
            "checksum":     fmt.Sprintf("%x", stateChecksum),
            "size":         len(stateData),
            "last_seq_num": state.LastSeqNum,
        })

    return nil
}

func (c *HydfsClient) ensureDirectory(path string) error {
    req := struct {
        Op   string `json:"op"`
        Path string `json:"path"`
    }{
        Op:   "MKDIR",
        Path: path,
    }
    
    return c.sendRequest(req)
}

func (w *Worker) recoverState() error {
    // 获取最新的状态文件
    stateFiles, err := w.HydfsClient.ListFiles("states/" + w.ID)
    if err != nil {
        return fmt.Errorf("failed to list state files: %v", err)
    }

    if len(stateFiles) == 0 {
        w.logEvent("STATE_RECOVERY", "No previous state found", nil)
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

    // 验证状态一致性
    if err := w.validateState(&state); err != nil {
        return fmt.Errorf("state validation failed: %v", err)
    }

    // 更新当前状态
    w.StateMutex.Lock()
    w.State = &state
    w.StateMutex.Unlock()

    w.logEvent("STATE_RECOVERED", 
        fmt.Sprintf("State recovered from %s", latestFile),
        map[string]interface{}{
            "last_seq_num":   state.LastSeqNum,
            "tuples_count":   len(state.ProcessedTuples),
            "aggregate_keys": len(state.AggregateState),
        })

    return nil
}

func (w *Worker) validateState(state *State) error {
    if state.ProcessedTuples == nil {
        state.ProcessedTuples = make(map[string]bool)
    }
    if state.AggregateState == nil {
        state.AggregateState = make(map[string]interface{})
    }

    // 对于聚合角色，验证计数值的有效性
    if w.Role == "aggregate" {
        for category, value := range state.AggregateState {
            count, ok := value.(float64) // JSON反序列化后数字默认为float64
            if !ok {
                return fmt.Errorf("invalid count type for category %s", category)
            }
            if count < 0 {
                return fmt.Errorf("invalid count value %f for category %s", count, category)
            }
            // 将float64转换回int
            state.AggregateState[category] = int(count)
        }
    }

    return nil
}

// 故障检测和恢复
func (w *Worker) monitorPeers() {
    ticker := time.NewTicker(HeartbeatInterval)
    defer ticker.Stop()

    for {
        select {
        case <-w.StopChan:
            return
        case <-ticker.C:
            w.checkPeerHealth()
        }
    }
}

func (w *Worker) checkPeerHealth() {
    w.memberMutex.Lock()
    defer w.memberMutex.Unlock()

    now := time.Now()
    for id, member := range w.members {
        if id == w.ID {
            continue
        }

        if member.Status == StatusNormal && 
           now.Sub(member.LastHeartbeat) > FailureTimeout {
            // 标记为失败
            member.Status = StatusFailed
            w.logEvent("PEER_FAILED", 
                fmt.Sprintf("Peer %s marked as failed", id),
                map[string]interface{}{
                    "last_heartbeat": member.LastHeartbeat,
                    "timeout":        FailureTimeout,
                })

            // 触发故障恢复
            go w.handlePeerFailure(id)
        }
    }
}

func (w *Worker) handlePeerFailure(failedID string) {
    // 记录故障处理开始
    w.logEvent("FAILURE_RECOVERY_STARTED", 
        fmt.Sprintf("Starting recovery for failed peer %s", failedID),
        nil)

    // 如果是聚合角色，需要重新分配状态
    if w.Role == "aggregate" {
        if err := w.redistributeState(failedID); err != nil {
            w.logError("state_redistribution_failed", err)
            return
        }
    }

    // 重新分配任务
    if err := w.reassignTasks(failedID); err != nil {
        w.logError("task_reassignment_failed", err)
        return
    }

    w.logEvent("FAILURE_RECOVERY_COMPLETED", 
        fmt.Sprintf("Completed recovery for failed peer %s", failedID),
        nil)
}

func (w *Worker) reassignTasks(failedID string) error {
    // 获取活跃节点列表
    activeNodes := make([]string, 0)
    w.memberMutex.RLock()
    for id, member := range w.members {
        if id != failedID && member.Status == StatusNormal {
            activeNodes = append(activeNodes, id)
        }
    }
    w.memberMutex.RUnlock()

    if len(activeNodes) == 0 {
        return errors.New("no active nodes available for task reassignment")
    }

    // 通知其他节点重新分配任务
    msg := Message{
        Type:      "TASK_REASSIGN",
        SenderID:  w.ID,
        Timestamp: time.Now(),
        Data: map[string]interface{}{
            "failed_node": failedID,
        },
    }

    for _, nodeID := range activeNodes {
        if err := w.sendMessage(nodeID, msg); err != nil {
            w.logError("task_reassignment_failed", 
                fmt.Errorf("failed to notify %s: %v", nodeID, err))
        }
    }

    return nil
}


func (w *Worker) redistributeState(failedID string) error {
    w.StateMutex.Lock()
    defer w.StateMutex.Unlock()

    // 获取活跃节点列表
    activeNodes := make([]string, 0)
    w.memberMutex.RLock()
    for id, member := range w.members {
        if id != failedID && member.Status == StatusNormal {
            activeNodes = append(activeNodes, id)
        }
    }
    w.memberMutex.RUnlock()

    if len(activeNodes) == 0 {
        return errors.New("no active nodes available for state redistribution")
    }

    // 重新分配状态
    for category, count := range w.State.AggregateState {
        // 使用一致性哈希或简单的取模来决定新的负责节点
        targetNode := activeNodes[int(hashString(category))%len(activeNodes)]
        if targetNode == w.ID {
            continue // 已经在当前节点上
        }

        // 发送状态更新消息给目标节点
        msg := Message{
            Type:      MsgStateSync,
            SenderID:  w.ID,
            Timestamp: time.Now(),
            Data: map[string]interface{}{
                "category": category,
                "count":    count,
            },
        }

        if err := w.sendMessage(targetNode, msg); err != nil {
            return fmt.Errorf("failed to redistribute state to %s: %v", targetNode, err)
        }
    }

    return nil
}

func hashString(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}


// 日志记录实现
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
    return w.HydfsClient.AppendFile(logPath, append(logLine, '\n'))
}

func (w *Worker) logEvent(eventType, message string, data interface{}) {
    select {
    case w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Level:     "INFO",
        Type:      eventType,
        Message:   message,
        Data:      data,
    }:
    default:
        w.Logger.Printf("Warning: Log channel full, dropped event: %s", eventType)
    }
}

func (w *Worker) logError(errType string, err error) {
    select {
    case w.LogChan <- &LogEntry{
        Timestamp: time.Now(),
        Level:     "ERROR",
        Type:      errType,
        Message:   err.Error(),
    }:
    default:
        w.Logger.Printf("Warning: Log channel full, dropped error: %s - %v", errType, err)
    }
}

// 主程序
func main() {
    // 解析命令行参数
    options := parseCommandLine()
    if err := validateOptions(options); err != nil {
        log.Fatalf("Invalid options: %v", err)
    }

    // 创建工作目录
    if err := createWorkDirs(options); err != nil {
        log.Fatalf("Failed to create work directories: %v", err)
    }

    // 根据角色启动不同类型的节点
    switch options.Role {
    case "leader":
        if err := runLeader(options); err != nil {
            log.Fatalf("Leader failed: %v", err)
        }
    case "worker":
        if err := runWorker(options); err != nil {
            log.Fatalf("Worker failed: %v", err)
        }
    default:
        log.Fatalf("Unknown role: %s", options.Role)
    }
}

func parseCommandLine() *CommandLineOptions {
    opts := &CommandLineOptions{}
    
    flag.StringVar(&opts.NodeID, "id", "", "Node ID")
    flag.StringVar(&opts.Role, "role", "", "Node role (leader/worker)")
    flag.StringVar(&opts.Addr, "addr", "localhost", "Listen address")
    flag.IntVar(&opts.Port, "port", BasePort, "Listen port")
    flag.StringVar(&opts.SourceFile, "src", "", "Source file path")
    flag.StringVar(&opts.DestFile, "dest", "", "Destination file path")
    flag.IntVar(&opts.NumTasks, "tasks", 3, "Number of tasks")
    flag.StringVar(&opts.ParamX, "param-x", "", "Parameter X value")
    flag.StringVar(&opts.HydfsAddr, "hydfs-addr", "localhost", "HyDFS server address")
    flag.IntVar(&opts.HydfsPort, "hydfs-port", FileServerPort, "HyDFS server port")
    flag.StringVar(&opts.LogDir, "log-dir", "logs", "Log directory")
    
    flag.Parse()
    
    return opts
}

func validateOptions(opts *CommandLineOptions) error {
    if opts.NodeID == "" {
        return errors.New("node ID is required")
    }
    if opts.Role == "" {
        return errors.New("role is required")
    }
    if opts.Role == "leader" {
        if opts.SourceFile == "" {
            return errors.New("source file is required for leader")
        }
        if opts.DestFile == "" {
            return errors.New("destination file is required for leader")
        }
        if opts.NumTasks < 1 {
            return errors.New("number of tasks must be positive")
        }
    }
    return nil
}

func createWorkDirs(opts *CommandLineOptions) error {
    dirs := []string{
        opts.LogDir,
        "states",
        "logs",
        filepath.Join("states", opts.NodeID),
        filepath.Join("logs", opts.NodeID),
    }

    for _, dir := range dirs {
        if err := os.MkdirAll(dir, 0755); err != nil {
            return fmt.Errorf("failed to create directory %s: %v", dir, err)
        }
    }
    return nil
}

func runLeader(opts *CommandLineOptions) error {
    // 创建leader配置
    config := WorkerConfig{
        ID:         opts.NodeID,
        Role:       opts.Role,
        Addr:       opts.Addr,
        Port:       opts.Port,
        BatchSize:  DefaultBatchSize,
        ParamX:     opts.ParamX,
        HydfsAddr:  opts.HydfsAddr,
        HydfsPort:  opts.HydfsPort,
        LogDir:     opts.LogDir,
    }

    // 创建并启动leader
    leader, err := NewWorker(config)
    if err != nil {
        return err
    }

    // 设置信号处理
    setupSignalHandler(leader)

    // 启动leader
    if err := leader.Start(); err != nil {
        return err
    }

    // 等待退出信号
    <-leader.StopChan
    return nil
}

func runWorker(opts *CommandLineOptions) error {
    config := WorkerConfig{
        ID:         opts.NodeID,
        Role:       opts.Role,
        Addr:       opts.Addr,
        Port:       opts.Port,
        BatchSize:  DefaultBatchSize,
        ParamX:     opts.ParamX,
        HydfsAddr:  opts.HydfsAddr,
        HydfsPort:  opts.HydfsPort,
        LogDir:     opts.LogDir,
    }

    worker, err := NewWorker(config)
    if err != nil {
        return err
    }

    // 设置信号处理
    setupSignalHandler(worker)

    // 启动worker
    if err := worker.Start(); err != nil {
        return err
    }

    // 等待退出信号
    <-worker.StopChan
    return nil
}

func setupSignalHandler(w *Worker) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        sig := <-sigChan
        w.logEvent("SHUTDOWN_INITIATED", 
            fmt.Sprintf("Received signal %s", sig),
            nil)
        close(w.StopChan)
    }()
}
