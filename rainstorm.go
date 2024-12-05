package main

import (
    "bufio"
    "encoding/csv"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

// 系统常量
const (
    // 配置
    DefaultBatchSize  = 100
    HeartbeatInterval = 2 * time.Second
    FailureTimeout    = 10 * time.Second
    ProcessTimeout    = 5 * time.Second
    maxRetries       = 3

    // 端口配置
    BasePort         = 5000
    FileServerPort   = 5001
    HeartbeatPort    = 5002
)

// 错误定义
var (
    ErrFileNotFound     = errors.New("file not found")
    ErrDuplicateTuple   = errors.New("duplicate tuple")
    ErrNodeNotFound     = errors.New("node not found")
    ErrInvalidOperation = errors.New("invalid operation")
)

// 基本数据结构
type Tuple struct {
    ID        string      `json:"id"`
    Key       string      `json:"key"`
    Value     interface{} `json:"value"`
    Timestamp time.Time   `json:"timestamp"`
    BatchID   string      `json:"batch_id"`
    Source    string      `json:"source"`
}

type Batch struct {
    ID            string          `json:"id"`
    Tuples        []*Tuple        `json:"tuples"`
    ProcessedIDs  map[string]bool `json:"processed_ids"`
    NextStageAcks map[string]bool `json:"next_stage_acks"`
    CreateTime    time.Time       `json:"create_time"`
}

// 操作符接口
type Operator interface {
    Process(tuple *Tuple) ([]*Tuple, error)
    GetState() map[string]interface{}
    LoadState(state map[string]interface{}) error
}

// Worker状态
type WorkerState struct {
    ProcessedTuples map[string]bool       `json:"processed_tuples"`
    AggregateState  map[string]interface{} `json:"aggregate_state"`
    LastSeqNum      int64                 `json:"last_seq_num"`
}

// 节点状态
type NodeStatus int

const (
    StatusNormal NodeStatus = iota 
    StatusSuspected
    StatusFailed
)

// 成员信息
type MemberInfo struct {
    ID            string
    Address       string
    Port          int
    Status        NodeStatus
    LastHeartbeat time.Time
}

// Worker配置
type WorkerConfig struct {
    ID          string
    Role        string // "source", "transform", "aggregate"
    BatchSize   int
    RangeStart  int
    RangeEnd    int
    ParamX      string
}

type Worker struct {
    ID              string
    Role            string
    Config          WorkerConfig
    
    // 数据流
    InputChan       chan *Tuple
    OutputChan      chan *Tuple
    errorChan       chan error
    stopChan        chan struct{}
    
    // 状态管理
    ProcessedTuples map[string]bool
    State          map[string]interface{}
    lastSeqNum     int64
    stateMutex     sync.RWMutex
    
    // 批处理
    currentBatch    *Batch
    batchesInFlight map[string]*Batch
    batchMutex      sync.RWMutex

    // 网络
    listener        net.Listener
    members        map[string]*MemberInfo
    memberMutex    sync.RWMutex
    
    // 日志
    logger         *log.Logger
}

func NewWorker(config WorkerConfig) *Worker {
    w := &Worker{
        ID:              config.ID,
        Role:            config.Role,
        Config:          config,
        
        InputChan:       make(chan *Tuple, 1000),
        OutputChan:      make(chan *Tuple, 1000),
        errorChan:       make(chan error, 100),
        stopChan:        make(chan struct{}),
        
        ProcessedTuples: make(map[string]bool),
        State:          make(map[string]interface{}),
        batchesInFlight: make(map[string]*Batch),
        members:         make(map[string]*MemberInfo),
        
        logger:         log.New(os.Stdout, fmt.Sprintf("[Worker %s] ", config.ID), log.LstdFlags),
    }
    
    return w
}

func (w *Worker) Start() error {
    // 启动网络监听
    if err := w.startNetwork(); err != nil {
        return err
    }

    // 启动处理循环
    go w.processLoop()
    go w.heartbeatLoop()
    go w.stateSyncLoop()

    w.logger.Printf("Worker started [Role: %s]", w.Role)
    return nil
}

func (w *Worker) processLoop() {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-w.stopChan:
            return

        case tuple := <-w.InputChan:
            if err := w.processTuple(tuple); err != nil {
                w.logError("process_tuple_failed", err)
            }

        case <-ticker.C:
            if w.Role == "source" {
                w.processSource()
            }
            w.checkAndSendBatch()
        }
    }
}

func (w *Worker) processTuple(tuple *Tuple) error {
    // 检查重复
    if w.isDuplicate(tuple.ID) {
        w.logger.Printf("Duplicate tuple detected: %s", tuple.ID)
        return nil
    }

    var results []*Tuple
    var err error

    // 根据角色处理tuple
    switch w.Role {
    case "source":
        results, err = w.processSourceTuple(tuple)
    case "transform":
        results, err = w.processFilterTuple(tuple)
    case "aggregate":
        results, err = w.processCountTuple(tuple)
    }

    if err != nil {
        return err
    }

    // 标记为已处理
    w.markProcessed(tuple.ID)

    // 添加到当前批次
    return w.addToBatch(results)
}

func (w *Worker) processSourceTuple(tuple *Tuple) ([]*Tuple, error) {
    line, ok := tuple.Value.(string)
    if !ok {
        return nil, fmt.Errorf("invalid source tuple value type")
    }

    // 创建新tuple
    newTuple := &Tuple{
        ID:        fmt.Sprintf("%s-%d", w.ID, w.lastSeqNum),
        Key:       line,
        Value:     line,
        Timestamp: time.Now(),
        BatchID:   fmt.Sprintf("batch-%s-%d", w.ID, w.lastSeqNum/w.Config.BatchSize),
        Source:    w.ID,
    }

    return []*Tuple{newTuple}, nil
}

func (w *Worker) processFilterTuple(tuple *Tuple) ([]*Tuple, error) {
    line := tuple.Value.(string)
    
    // 解析CSV
    reader := csv.NewReader(strings.NewReader(line))
    fields, err := reader.Read()
    if err != nil {
        return nil, err
    }

    // 检查是否包含Pattern X
    if !strings.Contains(line, w.Config.ParamX) {
        return nil, nil
    }

    // 提取OBJECTID和Sign_Type
    if len(fields) < 4 {
        return nil, fmt.Errorf("insufficient fields")
    }

    objectID := fields[0]
    signType := fields[3]
    
    // 创建结果tuple
    result := &Tuple{
        ID:        fmt.Sprintf("%s-filtered-%d", w.ID, w.lastSeqNum),
        Key:       objectID,
        Value:     fmt.Sprintf("%s,%s", objectID, signType),
        Timestamp: time.Now(),
        BatchID:   tuple.BatchID,
        Source:    w.ID,
    }

    return []*Tuple{result}, nil
}

func (w *Worker) processCountTuple(tuple *Tuple) ([]*Tuple, error) {
    line := tuple.Value.(string)
    
    // 解析CSV
    reader := csv.NewReader(strings.NewReader(line))
    fields, err := reader.Read()
    if err != nil {
        return nil, err
    }

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
    w.stateMutex.Lock()
    if w.State[category] == nil {
        w.State[category] = 0
    }
    count := w.State[category].(int) + 1
    w.State[category] = count
    w.stateMutex.Unlock()

    // 创建结果tuple
    result := &Tuple{
        ID:        fmt.Sprintf("%s-count-%d", w.ID, w.lastSeqNum),
        Key:       category,
        Value:     count,
        Timestamp: time.Now(),
        BatchID:   tuple.BatchID,
        Source:    w.ID,
    }

    return []*Tuple{result}, nil
}

// 批处理相关方法
func (w *Worker) addToBatch(tuples []*Tuple) error {
    w.batchMutex.Lock()
    defer w.batchMutex.Unlock()

    if w.currentBatch == nil {
        w.currentBatch = &Batch{
            ID:            fmt.Sprintf("batch-%s-%d", w.ID, w.lastSeqNum/w.Config.BatchSize),
            Tuples:        make([]*Tuple, 0),
            ProcessedIDs:  make(map[string]bool),
            NextStageAcks: make(map[string]bool),
            CreateTime:    time.Now(),
        }
    }

    for _, tuple := range tuples {
        w.currentBatch.Tuples = append(w.currentBatch.Tuples, tuple)
    }

    // 如果批次已满，发送它
    if len(w.currentBatch.Tuples) >= w.Config.BatchSize {
        return w.sendCurrentBatch()
    }

    return nil
}

func (w *Worker) sendCurrentBatch() error {
    if w.currentBatch == nil || len(w.currentBatch.Tuples) == 0 {
        return nil
    }

    batch := w.currentBatch
    w.currentBatch = nil
    w.batchesInFlight[batch.ID] = batch

    // 发送tuples
    for _, tuple := range batch.Tuples {
        select {
        case w.OutputChan <- tuple:
        case <-time.After(ProcessTimeout):
            return fmt.Errorf("timeout sending tuple")
        }
    }

    w.logger.Printf("Sent batch %s with %d tuples", batch.ID, len(batch.Tuples))
    return nil
}


// Leader 结构体定义
type Leader struct {
    ID            string
    Workers       map[string]*Worker
    workerMutex   sync.RWMutex
    
    // 任务分配
    assignments   map[string]string  // workerID -> task type
    taskRanges    map[string]struct {
        start    int
        end      int
    }
    
    // 配置
    numTasks      int
    sourceFile    string
    destFile      string
    paramX        string
    
    // 网络
    listener      net.Listener
    members       map[string]*MemberInfo
    memberMutex   sync.RWMutex
    
    // 通道
    errorChan     chan error
    stopChan      chan struct{}
    
    // 日志
    logger        *log.Logger
}

type LeaderConfig struct {
    NumTasks   int
    SourceFile string
    DestFile   string
    ParamX     string
}

func NewLeader(id string, config LeaderConfig) *Leader {
    return &Leader{
        ID:           id,
        Workers:      make(map[string]*Worker),
        assignments:  make(map[string]string),
        taskRanges:   make(map[string]struct{start, end int}),
        numTasks:     config.NumTasks,
        sourceFile:   config.SourceFile,
        destFile:     config.DestFile,
        paramX:       config.ParamX,
        members:      make(map[string]*MemberInfo),
        errorChan:    make(chan error, 100),
        stopChan:     make(chan struct{}),
        logger:       log.New(os.Stdout, fmt.Sprintf("[Leader %s] ", id), log.LstdFlags),
    }
}

func (l *Leader) Start() error {
    // 启动网络服务
    if err := l.startNetwork(); err != nil {
        return err
    }

    // 计算任务划分
    if err := l.calculateRanges(); err != nil {
        return err
    }

    // 初始化workers
    if err := l.initializeWorkers(); err != nil {
        return err
    }

    // 启动监控
    go l.monitorWorkers()
    go l.heartbeatChecker()

    l.logger.Printf("Leader started with %d tasks", l.numTasks)
    return nil
}

// 网络相关方法
func (l *Leader) startNetwork() error {
    addr := fmt.Sprintf(":%d", BasePort)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        return fmt.Errorf("failed to start listener: %v", err)
    }
    l.listener = listener

    go l.handleConnections()
    return nil
}

func (l *Leader) handleConnections() {
    for {
        conn, err := l.listener.Accept()
        if err != nil {
            select {
            case <-l.stopChan:
                return
            default:
                l.logger.Printf("Accept error: %v", err)
                continue
            }
        }
        go l.handleConnection(conn)
    }
}

func (l *Leader) handleConnection(conn net.Conn) {
    defer conn.Close()

    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        l.logger.Printf("Failed to decode message: %v", err)
        return
    }

    var response Message
    switch msg.Type {
    case "JOIN":
        response = l.handleJoinRequest(msg)
    case "HEARTBEAT":
        response = l.handleHeartbeat(msg)
    case "WORKER_STATUS":
        response = l.handleWorkerStatus(msg)
    default:
        response = Message{Type: "ERROR", Data: "unknown message type"}
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        l.logger.Printf("Failed to send response: %v", err)
    }
}

// 任务管理方法
func (l *Leader) calculateRanges() error {
    fileInfo, err := os.Stat(l.sourceFile)
    if err != nil {
        return err
    }

    // 读取文件计算行数
    file, err := os.Open(l.sourceFile)
    if err != nil {
        return err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    lineCount := 0
    for scanner.Scan() {
        lineCount++
    }

    if err := scanner.Err(); err != nil {
        return err
    }

    // 计算每个任务的行数范围
    linesPerTask := lineCount / l.numTasks
    remainder := lineCount % l.numTasks

    start := 0
    for i := 0; i < l.numTasks; i++ {
        size := linesPerTask
        if remainder > 0 {
            size++
            remainder--
        }

        l.taskRanges[fmt.Sprintf("source-%d", i)] = struct{
            start int
            end   int
        }{
            start: start,
            end:   start + size - 1,
        }
        
        start += size
    }

    return nil
}

func (l *Leader) initializeWorkers() error {
    // 创建source workers
    for i := 0; i < l.numTasks; i++ {
        id := fmt.Sprintf("source-%d", i)
        range_ := l.taskRanges[id]
        
        config := WorkerConfig{
            ID:         id,
            Role:       "source",
            BatchSize:  DefaultBatchSize,
            RangeStart: range_.start,
            RangeEnd:   range_.end,
        }
        
        worker := NewWorker(config)
        l.Workers[id] = worker
    }

    // 创建transform workers
    for i := 0; i < l.numTasks; i++ {
        id := fmt.Sprintf("transform-%d", i)
        config := WorkerConfig{
            ID:        id,
            Role:      "transform",
            BatchSize: DefaultBatchSize,
            ParamX:    l.paramX,
        }
        
        worker := NewWorker(config)
        l.Workers[id] = worker
    }

    // 创建aggregate workers
    for i := 0; i < l.numTasks; i++ {
        id := fmt.Sprintf("aggregate-%d", i)
        config := WorkerConfig{
            ID:        id,
            Role:      "aggregate",
            BatchSize: DefaultBatchSize,
            ParamX:    l.paramX,
        }
        
        worker := NewWorker(config)
        l.Workers[id] = worker
    }

    // 启动所有workers
    for _, worker := range l.Workers {
        if err := worker.Start(); err != nil {
            return fmt.Errorf("failed to start worker %s: %v", worker.ID, err)
        }
    }

    return nil
}

// 监控和故障处理
func (l *Leader) monitorWorkers() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-l.stopChan:
            return
        case <-ticker.C:
            l.checkWorkerHealth()
        case err := <-l.errorChan:
            l.logger.Printf("Worker error: %v", err)
        }
    }
}

func (l *Leader) checkWorkerHealth() {
    l.workerMutex.Lock()
    defer l.workerMutex.Unlock()

    now := time.Now()
    for id, worker := range l.Workers {
        l.memberMutex.RLock()
        member, exists := l.members[id]
        l.memberMutex.RUnlock()

        if !exists {
            continue
        }

        if member.Status == StatusNormal && 
           now.Sub(member.LastHeartbeat) > FailureTimeout {
            l.handleWorkerFailure(id)
        }
    }
}

// 故障恢复部分
func (l *Leader) handleWorkerFailure(failedID string) {
    worker := l.Workers[failedID]
    l.logger.Printf("Worker %s (%s) failure detected", failedID, worker.Role)

    // 根据角色处理故障
    switch worker.Role {
    case "source":
        l.handleSourceFailure(failedID)
    case "transform":
        l.handleTransformFailure(failedID)
    case "aggregate":
        l.handleAggregateFailure(failedID)
    }
}

func (l *Leader) handleSourceFailure(failedID string) {
    range_ := l.taskRanges[failedID]
    newID := fmt.Sprintf("%s-recovery", failedID)
    
    config := WorkerConfig{
        ID:         newID,
        Role:       "source",
        BatchSize:  DefaultBatchSize,
        RangeStart: range_.start,
        RangeEnd:   range_.end,
    }
    
    worker := NewWorker(config)
    
    delete(l.Workers, failedID)
    l.Workers[newID] = worker
    
    if err := worker.Start(); err != nil {
        l.logger.Printf("Failed to recover source worker: %v", err)
        return
    }

    l.logger.Printf("Source worker recovered as %s", newID)
}

func (l *Leader) handleTransformFailure(failedID string) {
    newID := fmt.Sprintf("%s-recovery", failedID)
    
    config := WorkerConfig{
        ID:        newID,
        Role:      "transform",
        BatchSize: DefaultBatchSize,
        ParamX:    l.paramX,
    }
    
    worker := NewWorker(config)
    
    delete(l.Workers, failedID)
    l.Workers[newID] = worker
    
    if err := worker.Start(); err != nil {
        l.logger.Printf("Failed to recover transform worker: %v", err)
        return
    }

    l.logger.Printf("Transform worker recovered as %s", newID)
}

func (l *Leader) handleAggregateFailure(failedID string) {
    newID := fmt.Sprintf("%s-recovery", failedID)
    
    config := WorkerConfig{
        ID:        newID,
        Role:      "aggregate",
        BatchSize: DefaultBatchSize,
        ParamX:    l.paramX,
    }
    
    worker := NewWorker(config)
    
    // 恢复状态
    if oldWorker, ok := l.Workers[failedID]; ok {
        worker.State = oldWorker.State
        worker.ProcessedTuples = oldWorker.ProcessedTuples
        worker.lastSeqNum = oldWorker.lastSeqNum
    }
    
    delete(l.Workers, failedID)
    l.Workers[newID] = worker
    
    if err := worker.Start(); err != nil {
        l.logger.Printf("Failed to recover aggregate worker: %v", err)
        return
    }

    l.logger.Printf("Aggregate worker recovered as %s", newID)
}

// 主程序
func main() {
    // 解析命令行参数
    var (
        nodeID    = flag.String("id", "", "Node ID")
        role      = flag.String("role", "", "Node role (leader/worker)")
        op1Exe    = flag.String("op1", "", "First operator (filter=X or count=X)")
        op2Exe    = flag.String("op2", "", "Second operator")
        srcFile   = flag.String("src", "", "Source file")
        destFile  = flag.String("dest", "", "Destination file")
        numTasks  = flag.Int("tasks", 3, "Number of tasks")
        joinAddr  = flag.String("join", "", "Leader address to join")
    )
    
    flag.Parse()

    if *nodeID == "" {
        log.Fatal("Node ID is required")
    }

    // 设置日志
    log.SetPrefix(fmt.Sprintf("[%s] ", *nodeID))
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)

    // 根据角色启动节点
    if *role == "leader" {
        if *srcFile == "" || *destFile == "" {
            log.Fatal("Source and destination files are required for leader")
        }

        // 解析操作符
        var paramX string
        if strings.HasPrefix(*op1Exe, "filter=") {
            paramX = strings.TrimPrefix(*op1Exe, "filter=")
        } else if strings.HasPrefix(*op1Exe, "count=") {
            paramX = strings.TrimPrefix(*op1Exe, "count=")
        }

        // 创建并启动leader
        config := LeaderConfig{
            NumTasks:   *numTasks,
            SourceFile: *srcFile,
            DestFile:   *destFile,
            ParamX:     paramX,
        }
        
        leader := NewLeader(*nodeID, config)
        if err := leader.Start(); err != nil {
            log.Fatalf("Failed to start leader: %v", err)
        }

        // 等待信号
        waitForSignal(leader)

    } else {
        if *joinAddr == "" {
            log.Fatal("Join address is required for worker")
        }

        // 创建并启动worker
        worker := NewWorker(WorkerConfig{
            ID:        *nodeID,
            Role:      "worker",
            BatchSize: DefaultBatchSize,
        })

        if err := worker.Start(); err != nil {
            log.Fatalf("Failed to start worker: %v", err)
        }

        // 加入集群
        if err := worker.joinCluster(*joinAddr); err != nil {
            log.Fatalf("Failed to join cluster: %v", err)
        }

        // 等待信号
        waitForSignal(worker)
    }
}

// 工具函数
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

// 测试函数
func TestBasicFunctionality(t *testing.T) {
    // 启动leader
    leaderConfig := LeaderConfig{
        NumTasks:   3,
        SourceFile: "test_data/TrafficSigns_1000.csv",
        DestFile:   "test_output.txt",
        ParamX:     "Stop",
    }
    
    leader := NewLeader("leader-test", leaderConfig)
    if err := leader.Start(); err != nil {
        t.Fatalf("Failed to start leader: %v", err)
    }
    defer leader.Stop()

    // 等待处理完成
    time.Sleep(5 * time.Second)

    // 验证输出
    output, err := os.ReadFile("test_output.txt")
    if err != nil {
        t.Fatalf("Failed to read output file: %v", err)
    }

    // 检查结果
    if !strings.Contains(string(output), "Stop") {
        t.Error("Expected output to contain 'Stop'")
    }
}

func TestFailureRecovery(t *testing.T) {
    // 启动leader
    leader := NewLeader("leader-test", LeaderConfig{
        NumTasks:   3,
        SourceFile: "test_data/TrafficSigns_1000.csv",
        DestFile:   "test_output_failure.txt",
        ParamX:     "Stop",
    })
    
    if err := leader.Start(); err != nil {
        t.Fatalf("Failed to start leader: %v", err)
    }
    defer leader.Stop()

    // 等待系统稳定
    time.Sleep(2 * time.Second)

    // 模拟worker故障
    for id, worker := range leader.Workers {
        if worker.Role == "transform" {
            worker.Stop()
            delete(leader.Workers, id)
            break
        }
    }

    // 等待恢复完成
    time.Sleep(5 * time.Second)

    // 验证系统是否恢复
    if len(leader.Workers) != leader.numTasks*3 {
        t.Error("Expected all workers to be recovered")
    }
}
