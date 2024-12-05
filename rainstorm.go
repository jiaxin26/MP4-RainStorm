package main

import (
    "bufio"
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/base64"
    "encoding/csv"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "hash/fnv"
    "io"
    "log"
    "math/rand"
    "net"
    "os"
    "os/signal"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"
)

/*
本代码为MP4参考实现的示例。

主要功能点：
1. RainStorm框架：
   - Leader节点：负责任务分配（source/transform/aggregate），分区策略（hash分区），成员监控、故障恢复和调度。
   - Worker节点：执行具体处理逻辑（source从HyDFS读数据行、transform过滤行和提取字段、aggregate根据key聚合计数）。
   - 两级处理pipeline示例：source -> transform -> aggregate
   - 最终结果输出到控制台与HyDFS中。

2. Exactly-once语义：
   - 每条tuple有唯一ID（source分配），各stage处理后写日志到HyDFS
   - ACK机制：下游任务处理完成后向上游ACK，上游在超时未ACK重发
   - Worker从HyDFS日志恢复状态和已处理的tuple ID，防止重复处理

3. 故障检测与恢复：
   - Leader定期对所有Worker发心跳，未响应标记为失败
   - 失败后重新分配任务（包括恢复日志、状态）
   - Workers在重启时从日志中恢复状态和已处理记录

4. 演示测试要求：
   - Test 1：无故障运行（过滤含X的行并输出OBJECTID,Sign_Type；对指定Sign Post类型X进行Category聚合）
   - Test 2：在运行中kill两个非source的Worker，系统应恢复并保证exactly-once无重复输出
   - Test 3：针对有状态aggregate操作注入故障，并恢复状态
   - Test 4：展示HyDFS中某task的日志文件内容
   - Test 5：代码流检查

注意：
- 需搭配实际的HyDFS实现（MP3）和故障检测(MP2)使用。本示例中提供了HydfsClient的简化调用逻辑，需要开发者根据实际情况实现。
- 部分逻辑（如真正的任务调度、具体hash分区策略、实际的集群成员列表维护）在此示例中简化处理。实际需从配置文件或命令行参数传入集群拓扑信息。
*/

// -------------------- 常量与类型定义 --------------------

const (
    HeartbeatInterval = 2 * time.Second
    FailureTimeout    = 5 * time.Second
    ProcessTimeout    = 5 * time.Second
    StateCheckInterval= 5 * time.Second
    DefaultBatchSize  = 100

    MsgTuple          = "TUPLE"
    MsgBatch          = "BATCH"
    MsgAck            = "ACK"
    MsgError          = "ERROR"
    MsgHeartbeat      = "HEARTBEAT"
    MsgTaskAssign     = "TASK_ASSIGN"
    MsgTaskReassign   = "TASK_REASSIGN"
    MsgStateSync      = "STATE_SYNC"

    RoleLeader        = "leader"
    RoleWorker        = "worker"
    StageSource       = "source"
    StageTransform    = "transform"
    StageAggregate    = "aggregate"

    // 用于exactly-once去重的日志文件等
    LogDirPrefix      = "logs"
    StateDirPrefix    = "states"
    OutputDirPrefix   = "outputs"

    MaxRetries        = 3
)

type NodeStatus int
const (
    StatusNormal NodeStatus = iota
    StatusFailed
)

type Message struct {
    Type      string      `json:"type"`
    SenderID  string      `json:"sender_id"`
    Data      interface{} `json:"data"`
    Timestamp time.Time   `json:"timestamp"`
}

type Tuple struct {
    ID        string      `json:"id"`
    Key       string      `json:"key"`
    Value     interface{} `json:"value"`
    Timestamp time.Time   `json:"timestamp"`
    BatchID   string      `json:"batch_id"`
    Source    string      `json:"source"`
}

type Batch struct {
    ID      string   `json:"id"`
    Tuples  []*Tuple `json:"tuples"`
    // 对于故障恢复需要保留ProcessedIDs和已发出但未ACK的tuple ID等信息
}

type State struct {
    ProcessedTuples map[string]bool       `json:"processed_tuples"`
    AggregateState  map[string]int        `json:"aggregate_state"`
    LastSeqNum      int64                 `json:"last_seq_num"`
}

type WorkerConfig struct {
    ID          string
    Role        string // leader/worker
    Stage       string // source/transform/aggregate (for workers)
    Addr        string
    Port        int
    ParamX      string // 用于过滤条件或指定Sign Post类型
    HydfsAddr   string
    HydfsPort   int
    LogDir      string
    SrcFile     string
    DestFile    string
    NumTasks    int
    ClusterFile string // 集群拓扑配置文件，用于leader和worker获取集群信息
}

type MemberInfo struct {
    ID            string
    Address       string
    Port          int
    Status        NodeStatus
    LastHeartbeat time.Time
    Stage         string
}

type HydfsClient struct {
    Addr  string
    Port  int
    mutex sync.Mutex
}

func (c *HydfsClient) connect() (net.Conn, error) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    addr := fmt.Sprintf("%s:%d", c.Addr, c.Port)
    conn, err := net.DialTimeout("tcp", addr, ProcessTimeout)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to HyDFS server at %s: %v", addr, err)
    }
    return conn, nil
}

func (c *HydfsClient) sendRequest(op string, path string, data []byte) (bool, []byte, string, error) {
    req := map[string]interface{}{
        "op":   op,
        "path": path,
    }

    // 如果有数据需要传递（CREATE或APPEND）
    if len(data) > 0 {
        encodedData := base64.StdEncoding.EncodeToString(data)
        req["data"] = encodedData
    }

    conn, err := c.connect()
    if err != nil {
        return false, nil, "", err
    }
    defer conn.Close()

    // 发送请求
    enc := json.NewEncoder(conn)
    if err := enc.Encode(req); err != nil {
        return false, nil, "", fmt.Errorf("failed to send request: %v", err)
    }

    // 接收响应
    var resp struct {
        Success bool   `json:"success"`
        Data    string `json:"data"`
        Error   string `json:"error,omitempty"`
    }

    dec := json.NewDecoder(conn)
    if err := dec.Decode(&resp); err != nil {
        return false, nil, "", fmt.Errorf("failed to decode response: %v", err)
    }

    if !resp.Success {
        return false, nil, resp.Error, nil
    }

    // 对返回的Data进行base64解码（对READ和LIST返回值可能需要）
    var decoded []byte
    if resp.Data != "" {
        d, err := base64.StdEncoding.DecodeString(resp.Data)
        if err != nil {
            return false, nil, "", fmt.Errorf("failed to decode response data: %v", err)
        }
        decoded = d
    }

    return true, decoded, "", nil
}

func (l *Leader) loadClusterConfig(path string) error {
    file, err := os.Open(path)
    if err != nil {
        return fmt.Errorf("failed to open cluster config: %v", err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        fields := strings.Fields(line)
        // 假设格式：nodeID role stage hostname port
        if len(fields) < 5 {
            continue
        }

        nodeID := fields[0]
        role := fields[1]
        stage := fields[2]
        host := fields[3]
        p, err := strconv.Atoi(fields[4])
        if err != nil {
            return fmt.Errorf("invalid port number in cluster config line: %s", line)
        }

        // 仅在Leader中初始化所有节点信息
        // 对于Leader自身，也在此更新/确认自己的信息
        l.MemberMutex.Lock()
        l.Members[nodeID] = &MemberInfo{
            ID:            nodeID,
            Address:       host,
            Port:          p,
            Status:        StatusNormal,
            LastHeartbeat: time.Now(),
            Stage:         stage,
        }
        l.MemberMutex.Unlock()
    }

    if err := scanner.Err(); err != nil {
        return fmt.Errorf("error reading cluster config: %v", err)
    }

    return nil
}

func (w *Worker) loadClusterConfig(path string) error {
    file, err := os.Open(path)
    if err != nil {
        return fmt.Errorf("failed to open cluster config: %v", err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        fields := strings.Fields(line)
        if len(fields) < 5 {
            continue
        }
        nodeID := fields[0]
        role := fields[1]
        stage := fields[2]
        host := fields[3]
        p, err := strconv.Atoi(fields[4])
        if err != nil {
            return fmt.Errorf("invalid port number in cluster config line: %s", line)
        }

        w.MemberMutex.Lock()
        w.Members[nodeID] = &MemberInfo{
            ID: nodeID,
            Address: host,
            Port: p,
            Status: StatusNormal,
            LastHeartbeat: time.Now(),
            Stage: stage,
        }
        w.MemberMutex.Unlock()
    }

    if err := scanner.Err(); err != nil {
        return fmt.Errorf("error reading cluster config: %v", err)
    }
    return nil
}


func (c *HydfsClient) CreateFile(path string, data []byte) error {
    success, _, errMsg, err := c.sendRequest("CREATE", path, data)
    if err != nil {
        return err
    }
    if !success {
        return fmt.Errorf("HyDFS CREATE failed: %s", errMsg)
    }
    return nil
}

func (c *HydfsClient) AppendFile(path string, data []byte) error {
    success, _, errMsg, err := c.sendRequest("APPEND", path, data)
    if err != nil {
        return err
    }
    if !success {
        return fmt.Errorf("HyDFS APPEND failed: %s", errMsg)
    }
    return nil
}

func (c *HydfsClient) ReadFile(path string) ([]byte, error) {
    success, decoded, errMsg, err := c.sendRequest("READ", path, nil)
    if err != nil {
        return nil, err
    }
    if !success {
        return nil, fmt.Errorf("HyDFS READ failed: %s", errMsg)
    }
    return decoded, nil
}

func (c *HydfsClient) ListFiles(prefix string) ([]string, error) {
    success, decoded, errMsg, err := c.sendRequest("LIST", prefix, nil)
    if err != nil {
        return nil, err
    }
    if !success {
        return nil, fmt.Errorf("HyDFS LIST failed: %s", errMsg)
    }

    // 解码的decoded数据可能是JSON数组，需要解析
    var files []string
    if err := json.Unmarshal(decoded, &files); err != nil {
        return nil, fmt.Errorf("failed to unmarshal file list: %v", err)
    }

    return files, nil
}

// -------------------- Worker实现 --------------------

type Worker struct {
    Config       WorkerConfig
    Listener     net.Listener
    StopChan     chan struct{}

    State        *State
    StateMutex   sync.RWMutex

    // 输入/输出通道，实际中可能通过TCP传输，这里简化
    InputChan    chan *Tuple
    OutputChan   chan *Tuple
    AckChan      chan string // 接收ACK的通道
    LogChan      chan string

    HydfsClient  *HydfsClient

    Members      map[string]*MemberInfo
    MemberMutex  sync.RWMutex

    // 对于exactly-once：要有日志文件记录已处理tuples和状态
    ProcessedLogPath string
    StateLogPath     string

    ParamX      string // 来自Config，用于操作逻辑

    // 当前批处理
    CurrentBatch *Batch
    BatchMutex   sync.Mutex

    // 用于ACK重试
    pendingAcks  map[string]*Tuple
    ackMutex     sync.Mutex
}

func NewWorker(config WorkerConfig) (*Worker, error) {
    w := &Worker{
        Config:     config,
        StopChan:   make(chan struct{}),
        InputChan:  make(chan *Tuple, 1000),
        OutputChan: make(chan *Tuple, 1000),
        AckChan:    make(chan string, 1000),
        LogChan:    make(chan string, 1000),
        State: &State{
            ProcessedTuples: map[string]bool{},
            AggregateState:  map[string]int{},
            LastSeqNum:      0,
        },
        Members:    make(map[string]*MemberInfo),
        pendingAcks:make(map[string]*Tuple),
        ParamX:     config.ParamX,
    }

    w.HydfsClient = &HydfsClient{Addr: config.HydfsAddr, Port: config.HydfsPort}

    // 设置日志与状态路径
    w.ProcessedLogPath = filepath.Join("logs", w.Config.ID+"_processed.log")
    w.StateLogPath = filepath.Join("states", w.Config.ID+"_state.log")

    return w, nil
}

func (w *Worker) sourceReaderLoop() {

    for {
        select {
        case <-w.StopChan:
            return
        default:
            data, err := w.HydfsClient.ReadFile(w.Config.SrcFile)
            if err != nil {
                // 如果读取失败，等待一段时间重试
                log.Printf("[%s] Failed to read source file %s from HyDFS: %v",
                    w.Config.ID, w.Config.SrcFile, err)
                time.Sleep(2 * time.Second)
                continue
            }

            lines := bytes.Split(data, []byte("\n"))
            for i, line := range lines {
                // 去掉空行
                strLine := strings.TrimSpace(string(line))
                if strLine == "" {
                    continue
                }

                // 为每一行创建一个tuple，并放入InputChan
                tupleID := fmt.Sprintf("%s-line-%d", w.Config.ID, i)
                t := &Tuple{
                    ID: tupleID,
                    Key: fmt.Sprintf("%s:%d", w.Config.SrcFile, i),
                    Value: strLine,
                    Timestamp: time.Now(),
                    Source: w.Config.ID,
                }

                select {
                case w.InputChan <- t:
                    // 正常发送
                case <-w.StopChan:
                    return
                }

                // 控制发送速率，避免太快
                time.Sleep(10 * time.Millisecond)
            }
            break
        }
    }
}

func (w *Worker) Start() error {
    if err := w.recoverState(); err != nil {
        log.Printf("[%s] Failed to recover state: %v\n", w.Config.ID, err)
    }

    addr := fmt.Sprintf("%s:%d", w.Config.Addr, w.Config.Port)
    ln, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    w.Listener = ln

    if w.Config.Stage == StageSource && w.Config.SrcFile != "" {
        go w.sourceReaderLoop()
    }

    go w.acceptLoop()
    go w.processLoop()
    go w.outputLoop()
    go w.ackLoop()
    go w.statePersistenceLoop()

    log.Printf("[%s] Worker started at %s (stage=%s)\n", w.Config.ID, addr, w.Config.Stage)

    return nil
}

func (w *Worker) acceptLoop() {
    for {
        conn, err := w.Listener.Accept()
        if err != nil {
            select {
            case <-w.StopChan:
                return
            default:
                log.Printf("[%s] Accept error: %v\n", w.Config.ID, err)
                continue
            }
        }
        go w.handleConnection(conn)
    }
}

func (w *Worker) handleConnection(conn net.Conn) {
    defer conn.Close()
    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        w.sendError(conn, err.Error())
        return
    }

    switch msg.Type {
    case MsgTuple:
        // 单tuple直接处理
        data, _ := json.Marshal(msg.Data)
        var tuple Tuple
        json.Unmarshal(data, &tuple)
        w.InputChan <- &tuple
        w.sendAck(conn, "received")
    case MsgBatch:
        // 批次处理
        data, _ := json.Marshal(msg.Data)
        var batch Batch
        json.Unmarshal(data, &batch)
        for _, t := range batch.Tuples {
            w.InputChan <- t
        }
        w.sendAck(conn, "batch_received")
    case MsgAck:
        // 收到下游的ACK
        id, ok := msg.Data.(string)
        if ok {
            w.AckChan <- id
        }
        w.sendAck(conn, "ack_received")
    case MsgHeartbeat:
        // 心跳回复
        w.sendAck(conn, "alive")
    case MsgTaskAssign:
        // Leader给Worker分配任务（可以设定Stage, source file partition等）
        w.applyTaskAssignment(msg.Data)
        w.sendAck(conn, "task_assigned")
    case MsgTaskReassign:
        // 任务重分配
        w.applyTaskAssignment(msg.Data)
        w.sendAck(conn, "task_reassigned")
    case MsgStateSync:
        w.handleStateSync(msg.Data)
        w.sendAck(conn, "state_synced")
    default:
        w.sendError(conn, "unknown message type")
    }
}

func (w *Worker) sendError(conn net.Conn, errMsg string) {
    resp := Message{
        Type:      MsgError,
        SenderID:  w.Config.ID,
        Timestamp: time.Now(),
        Data:      errMsg,
    }
    json.NewEncoder(conn).Encode(resp)
}

func (w *Worker) sendAck(conn net.Conn, data interface{}) {
    resp := Message{
        Type:      MsgAck,
        SenderID:  w.Config.ID,
        Timestamp: time.Now(),
        Data:      data,
    }
    json.NewEncoder(conn).Encode(resp)
}

func (w *Worker) applyTaskAssignment(data interface{}) {
    // 根据数据更新自身的Stage、数据文件分区、ParamX等信息
    // 简化处理：假设data里有stage、paramX等
    m, ok := data.(map[string]interface{})
    if ok {
        if stg, ok := m["stage"].(string); ok {
            w.Config.Stage = stg
        }
        if px, ok := m["paramX"].(string); ok {
            w.ParamX = px
        }
        if src, ok := m["src_file"].(string); ok {
            w.Config.SrcFile = src
        }
        if dst, ok := m["dest_file"].(string); ok {
            w.Config.DestFile = dst
        }
        // 可根据NumTasks等配置进行初始化
    }
    log.Printf("[%s] Task assigned: stage=%s paramX=%s src=%s dest=%s\n",
        w.Config.ID, w.Config.Stage, w.ParamX, w.Config.SrcFile, w.Config.DestFile)
}

func (w *Worker) handleStateSync(data interface{}) {
    // 从data中提取要合并的状态（对有状态task，如aggregate）
    // 比如data里有category和count
    m, ok := data.(map[string]interface{})
    if !ok {
        return
    }
    if w.Config.Stage == StageAggregate {
        cat, cok := m["category"].(string)
        cnt, iok := m["count"].(float64)
        if cok && iok {
            w.StateMutex.Lock()
            w.State.AggregateState[cat] = w.State.AggregateState[cat] + int(cnt)
            w.StateMutex.Unlock()
            log.Printf("[%s] State synced: %s += %d\n", w.Config.ID, cat, int(cnt))
        }
    }
}

func (w *Worker) processLoop() {
    // 根据stage处理tuple
    for {
        select {
        case <-w.StopChan:
            return
        case t := <-w.InputChan:
            w.processTuple(t)
        }
    }
}

func (w *Worker) processTuple(tuple *Tuple) {
    // Exactly-once：检查是否已处理过
    w.StateMutex.RLock()
    if w.State.ProcessedTuples[tuple.ID] {
        w.StateMutex.RUnlock()
        // 丢弃重复
        return
    }
    w.StateMutex.RUnlock()

    // 未处理过，执行操作
    var results []*Tuple
    switch w.Config.Stage {
    case StageSource:
        // Source: 从HyDFS中读数据行，然后发往下游(实际中source是主动读文件而不是被动接收，这里简化)
        // 在demo中source将从SrcFile读行并产出tuple
        // 这里假设已经拿到tuple是文件的一行记录
        // Source的工作在真实系统中是持续读文件并发送下游，这里仅示意
        // 此处假设tuple.Value就是一行
        results = []*Tuple{tuple}
    case StageTransform:
        // Transform: 对行进行过滤，若包含ParamX则输出 (OBJECTID,Sign_Type)
        line, _ := tuple.Value.(string)
        if strings.Contains(line, w.ParamX) {
            // CSV解析
            reader := csv.NewReader(strings.NewReader(line))
            record, err := reader.Read()
            if err == nil && len(record) > 3 {
                objectID := record[2]
                signType := record[3]
                val := fmt.Sprintf("%s,%s", objectID, signType)
                outTuple := &Tuple{
                    ID: fmt.Sprintf("%s-%d", w.Config.ID, w.State.LastSeqNum),
                    Key: objectID,
                    Value: val,
                    Timestamp: time.Now(),
                    Source: w.Config.ID,
                }
                results = []*Tuple{outTuple}
            }
        }
    case StageAggregate:
        // Aggregate: 对特定Sign Post类型的记录，根据Category计数
        line, _ := tuple.Value.(string)
        reader := csv.NewReader(strings.NewReader(line))
        record, err := reader.Read()
        if err == nil && len(record) > 4 {
            signPost := record[3]
            category := record[4]
            if category == "" {
                category = "empty"
            } else if category == " " {
                category = "space"
            }
            if signPost == w.ParamX {
                // increment count
                w.StateMutex.Lock()
                w.State.AggregateState[category] = w.State.AggregateState[category] + 1
                count := w.State.AggregateState[category]
                w.StateMutex.Unlock()
                outTuple := &Tuple{
                    ID: fmt.Sprintf("%s-%d", w.Config.ID, w.State.LastSeqNum),
                    Key: category,
                    Value: count,
                    Timestamp: time.Now(),
                    Source: w.Config.ID,
                }
                results = []*Tuple{outTuple}
            }
        }
    default:
        // 未知stage不处理
        return
    }

    // 标记已处理
    w.StateMutex.Lock()
    w.State.ProcessedTuples[tuple.ID] = true
    w.State.LastSeqNum++
    w.StateMutex.Unlock()

    // 结果发送到OutputChan
    for _, r := range results {
        w.OutputChan <- r
    }
}

func (w *Worker) outputLoop() {
    // 将OutputChan中的tuple发送到下游（或最终输出）
    // 在最终stage（aggregate）中输出到console和HyDFS
    // 在中间stage发送给下游Worker

    // 实际中需要由Leader给出下游节点列表，这里简化为通过Stage判断
    // 假设固定拓扑: source -> transform -> aggregate
    // Leader将给每个Worker一个下游节点列表（在applyTaskAssignment中可加入）
    // 此处简化，假设如果是aggregate就输出到console和dest file，否则发给下一个stage（leader会提供下游地址）

    var nextStageNodes []MemberInfo
    var isFinalStage bool
    // 简化逻辑，根据Stage判断：source发给transform，transform发给aggregate，aggregate输出最终结果
    for {
        select {
        case <-w.StopChan:
            return
        case t := <-w.OutputChan:
            if w.Config.Stage == StageSource {
                // 找transform节点发送
                nextStageNodes = w.getStageNodes(StageTransform)
            } else if w.Config.Stage == StageTransform {
                // 找aggregate节点发送
                nextStageNodes = w.getStageNodes(StageAggregate)
            } else if w.Config.Stage == StageAggregate {
                // 最终输出
                isFinalStage = true
            }

            if isFinalStage {
                // 输出到console并写入HyDFS文件
                fmt.Printf("[%s] FINAL OUTPUT: %s=%v\n", w.Config.ID, t.Key, t.Value)
                if w.Config.DestFile != "" {
                    // 将结果append到DestFile
                    line := fmt.Sprintf("%s,%v\n", t.Key, t.Value)
                    w.HydfsClient.AppendFile(w.Config.DestFile, []byte(line))
                }
                // ack不需要发送给下游，因为没有下游
            } else {
                // 发送给下游阶段的任务（hash分区）
                if len(nextStageNodes) > 0 {
                    target := w.chooseNodeForTuple(nextStageNodes, t.Key)
                    // 发送tuple给target
                    if err := w.sendTuple(target, t); err != nil {
                        log.Printf("[%s] Failed to send tuple to %s: %v\n", w.Config.ID, target.ID, err)
                    } else {
                        // 等ACK
                        w.addPendingAck(t)
                    }
                }
            }
        }
    }
}

func (w *Worker) ackLoop() {
    // 处理下游ACK，去除pendingAcks
    for {
        select {
        case <-w.StopChan:
            return
        case ackID := <-w.AckChan:
            w.removePendingAck(ackID)
        }
    }
}

func (w *Worker) addPendingAck(t *Tuple) {
    w.ackMutex.Lock()
    w.pendingAcks[t.ID] = t
    w.ackMutex.Unlock()
    go w.retryOnTimeout(t)
}

func (w *Worker) removePendingAck(id string) {
    w.ackMutex.Lock()
    delete(w.pendingAcks, id)
    w.ackMutex.Unlock()
}

func (w *Worker) retryOnTimeout(t *Tuple) {
    // 如果在指定时间内未收到ACK，重发
    timer := time.NewTimer(ProcessTimeout)
    defer timer.Stop()
    <-timer.C
    w.ackMutex.Lock()
    _, stillPending := w.pendingAcks[t.ID]
    w.ackMutex.Unlock()
    if stillPending {
        // 重发
        // 找下游节点
        nextStageNodes := w.getStageNodesForResend()
        if len(nextStageNodes) > 0 {
            target := w.chooseNodeForTuple(nextStageNodes, t.Key)
            if err := w.sendTuple(target, t); err != nil {
                log.Printf("[%s] Retry send failed: %v\n", w.Config.ID, err)
            } else {
                // 再次等待ACK
                go w.retryOnTimeout(t)
            }
        }
    }
}

func (w *Worker) getStageNodes(stage string) []MemberInfo {
    w.MemberMutex.RLock()
    defer w.MemberMutex.RUnlock()
    var nodes []MemberInfo
    for _, m := range w.Members {
        if m.Stage == stage && m.Status == StatusNormal {
            nodes = append(nodes, *m)
        }
    }
    return nodes
}

func (w *Worker) getStageNodesForResend() []MemberInfo {
    // 重发时使用相同逻辑
    if w.Config.Stage == StageSource {
        return w.getStageNodes(StageTransform)
    } else if w.Config.Stage == StageTransform {
        return w.getStageNodes(StageAggregate)
    }
    return nil
}

func (w *Worker) chooseNodeForTuple(nodes []MemberInfo, key string) MemberInfo {
    // 简单hash分区
    h := fnv.New32a()
    h.Write([]byte(key))
    idx := int(h.Sum32()) % len(nodes)
    return nodes[idx]
}

func (w *Worker) sendTuple(target MemberInfo, t *Tuple) error {
    addr := fmt.Sprintf("%s:%d", target.Address, target.Port)
    conn, err := net.DialTimeout("tcp", addr, ProcessTimeout)
    if err != nil {
        return err
    }
    defer conn.Close()

    msg := Message{
        Type:      MsgTuple,
        SenderID:  w.Config.ID,
        Timestamp: time.Now(),
        Data:      t,
    }
    if err := json.NewEncoder(conn).Encode(msg); err != nil {
        return err
    }

    // 等回复
    var resp Message
    if err := json.NewDecoder(conn).Decode(&resp); err != nil {
        return err
    }
    if resp.Type == MsgError {
        return errors.New("received remote error: " + fmt.Sprintf("%v", resp.Data))
    }
    return nil
}

func (w *Worker) statePersistenceLoop() {
    ticker := time.NewTicker(StateCheckInterval)
    defer ticker.Stop()
    for {
        select {
        case <-w.StopChan:
            return
        case <-ticker.C:
            w.persistState()
        }
    }
}

func (w *Worker) persistState() {
    w.StateMutex.RLock()
    data, _ := json.Marshal(w.State)
    w.StateMutex.RUnlock()
    stateFile := filepath.Join("states", w.Config.ID+"_state.log")

    if err := w.HydfsClient.AppendFile(stateFile, data); err != nil {
        log.Printf("[%s] Failed to persist state: %v\n", w.Config.ID, err)
    }
}

func (w *Worker) recoverState() error {
    stateFile := filepath.Join("states", w.Config.ID+"_state.log")
    data, err := w.HydfsClient.ReadFile(stateFile)
    if err != nil || len(data)==0 {
        return nil
    }
    // 假设最后一行是最新的state
    lines := bytes.Split(data, []byte("\n"))
    if len(lines) == 0 {
        return nil
    }
    line := lines[len(lines)-1]
    var st State
    if err := json.Unmarshal(line, &st); err != nil {
        return err
    }
    w.StateMutex.Lock()
    w.State = &st
    w.StateMutex.Unlock()
    return nil
}

// -------------------- Leader实现 --------------------

type Leader struct {
    Config     WorkerConfig
    Listener   net.Listener
    StopChan   chan struct{}

    Members    map[string]*MemberInfo
    MemberMutex sync.RWMutex

    HydfsClient *HydfsClient
}

func NewLeader(config WorkerConfig) (*Leader, error) {
    l := &Leader{
        Config:    config,
        StopChan:  make(chan struct{}),
        Members:   make(map[string]*MemberInfo),
        HydfsClient: &HydfsClient{Addr: config.HydfsAddr, Port: config.HydfsPort},
    }
    return l, nil
}

func (l *Leader) Start() error {
    addr := fmt.Sprintf("%s:%d", l.Config.Addr, l.Config.Port)
    ln, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    l.Listener = ln
    go l.acceptLoop()
    go l.heartbeatLoop()
    go l.assignInitialTasks()

    log.Printf("[LEADER] started at %s\n", addr)
    return nil
}

func (l *Leader) acceptLoop() {
    for {
        conn, err := l.Listener.Accept()
        if err != nil {
            select {
            case <-l.StopChan:
                return
            default:
                log.Printf("[LEADER] Accept error: %v\n", err)
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
        l.sendError(conn, err.Error())
        return
    }

    switch msg.Type {
    case MsgHeartbeat:
        l.updateHeartbeat(msg.SenderID)
        l.sendAck(conn, "alive")
    case MsgError:
        // 接收来自worker的错误信息
        l.sendAck(conn, "noted")
    default:
        l.sendError(conn, "unknown message type for leader")
    }
}

func (l *Leader) sendError(conn net.Conn, errMsg string) {
    resp := Message{
        Type:      MsgError,
        SenderID:  l.Config.ID,
        Timestamp: time.Now(),
        Data:      errMsg,
    }
    json.NewEncoder(conn).Encode(resp)
}

func (l *Leader) sendAck(conn net.Conn, data interface{}) {
    resp := Message{
        Type:      MsgAck,
        SenderID:  l.Config.ID,
        Timestamp: time.Now(),
        Data:      data,
    }
    json.NewEncoder(conn).Encode(resp)
}

func (l *Leader) updateHeartbeat(id string) {
    l.MemberMutex.Lock()
    defer l.MemberMutex.Unlock()
    if m, ok := l.Members[id]; ok {
        m.LastHeartbeat = time.Now()
        m.Status = StatusNormal
    }
}

func (l *Leader) heartbeatLoop() {
    ticker := time.NewTicker(HeartbeatInterval)
    defer ticker.Stop()
    for {
        select {
        case <-l.StopChan:
            return
        case <-ticker.C:
            l.checkMembers()
        }
    }
}

func (l *Leader) checkMembers() {
    l.MemberMutex.Lock()
    defer l.MemberMutex.Unlock()
    now := time.Now()
    for _, m := range l.Members {
        if m.Status == StatusNormal && now.Sub(m.LastHeartbeat) > FailureTimeout {
            m.Status = StatusFailed
            // 重新分配任务
            go l.reassignTasks(m.ID)
        }
    }
}

func (l *Leader) assignInitialTasks() {
    // 从ClusterFile读取所有worker信息并分配任务
    // 简化处理：假设已有source,transform,aggregate各若干worker
    // 实际需根据NumTasks和Hydfs_src_file分块
    time.Sleep(2*time.Second) // 等待所有worker上线
    l.MemberMutex.Lock()
    defer l.MemberMutex.Unlock()

    // 简单分配：选几个worker为source, transform, aggregate
    var sources, transforms, aggregates []MemberInfo
    for _, m := range l.Members {
        if strings.Contains(m.ID, "src") {
            sources = append(sources, *m)
        } else if strings.Contains(m.ID, "transform") {
            transforms = append(transforms, *m)
        } else if strings.Contains(m.ID, "agg") {
            aggregates = append(aggregates, *m)
        }
    }

    // 给source分配src_file和paramX
    for i, src := range sources {
        data := map[string]interface{}{
            "stage": StageSource,
            "paramX": l.Config.ParamX,
            "src_file": l.Config.SrcFile,
            "dest_file": l.Config.DestFile,
        }
        l.sendTaskAssign(src, data)
        // 简化：实际应对多分片输入文件进行分配
        _ = i
    }

    // transform
    for _, tr := range transforms {
        data := map[string]interface{}{
            "stage": StageTransform,
            "paramX": l.Config.ParamX,
        }
        l.sendTaskAssign(tr, data)
    }

    // aggregate
    for _, ag := range aggregates {
        data := map[string]interface{}{
            "stage": StageAggregate,
            "paramX": l.Config.ParamX,
            "dest_file": l.Config.DestFile,
        }
        l.sendTaskAssign(ag, data)
    }

    log.Printf("[LEADER] Initial tasks assigned.\n")
}

func (l *Leader) sendTaskAssign(m MemberInfo, data interface{}) {
    l.sendMessage(m, MsgTaskAssign, data)
}

func (l *Leader) reassignTasks(failedID string) {
    log.Printf("[LEADER] Reassigning tasks from failed node %s\n", failedID)

    // 简化：从failedID中恢复任务到其他正常节点
    // 实际应从HyDFS日志中恢复状态并发送给其他worker
    // 此处只演示流程

    // 找到与failedID相同stage的另一个正常节点并同步状态
    l.MemberMutex.Lock()
    defer l.MemberMutex.Unlock()

    failedStage := ""
    for _, m := range l.Members {
        if m.ID == failedID {
            failedStage = m.Stage
            break
        }
    }

    var target *MemberInfo
    for _, m := range l.Members {
        if m.ID != failedID && m.Stage == failedStage && m.Status == StatusNormal {
            target = m
            break
        }
    }

    if target == nil {
        log.Printf("[LEADER] No active node to reassign tasks for stage %s\n", failedStage)
        return
    }

    data := map[string]interface{}{
        "stage": failedStage,
        "paramX": l.Config.ParamX,
        "src_file": l.Config.SrcFile,
        "dest_file": l.Config.DestFile,
    }
    l.sendMessage(*target, MsgTaskReassign, data)
}

func (l *Leader) sendMessage(m MemberInfo, msgType string, data interface{}) {
    addr := fmt.Sprintf("%s:%d", m.Address, m.Port)
    conn, err := net.DialTimeout("tcp", addr, ProcessTimeout)
    if err != nil {
        log.Printf("[LEADER] Failed to connect %s: %v\n", m.ID, err)
        return
    }
    defer conn.Close()

    msg := Message{
        Type: msgType,
        SenderID: l.Config.ID,
        Timestamp: time.Now(),
        Data: data,
    }
    if err := json.NewEncoder(conn).Encode(msg); err != nil {
        log.Printf("[LEADER] sendMessage error: %v\n", err)
    }

    var resp Message
    if err := json.NewDecoder(conn).Decode(&resp); err != nil {
        log.Printf("[LEADER] read response error: %v\n", err)
    }
}

// -------------------- 主函数 --------------------

var (
    nodeID     = flag.String("id", "", "Node ID")
    role       = flag.String("role", "", "Role: leader/worker")
    addr       = flag.String("addr", "localhost", "Listen address")
    port       = flag.Int("port", 5000, "Listen port")
    paramX     = flag.String("param-x", "", "Param X")
    hydfsAddr  = flag.String("hydfs-addr", "localhost", "HyDFS addr")
    hydfsPort  = flag.Int("hydfs-port", 5001, "HyDFS port")
    logDir     = flag.String("log-dir", "logs", "Log directory")
    srcFile    = flag.String("src", "", "Source file path")
    destFile   = flag.String("dest", "", "Destination file path")
    numTasks   = flag.Int("tasks", 3, "Number of tasks")
    clusterFile= flag.String("cluster-file", "cluster.conf", "Cluster config file")
)

func main() {
    flag.Parse()
    if *nodeID == "" || *role == "" {
        log.Fatal("Missing required flags: -id, -role")
    }

    config := WorkerConfig{
        ID: *nodeID,
        Role: *role,
        Addr: *addr,
        Port: *port,
        ParamX: *paramX,
        HydfsAddr: *hydfsAddr,
        HydfsPort: *hydfsPort,
        LogDir: *logDir,
        SrcFile: *srcFile,
        DestFile: *destFile,
        NumTasks: *numTasks,
        ClusterFile: *clusterFile,
    }

    if *role == RoleLeader {
        leader, err := NewLeader(config)
        if err != nil {
            log.Fatal(err)
        }
        if err := leader.Start(); err != nil {
            log.Fatal(err)
        }
        waitSignal(leader.StopChan)
    } else if *role == RoleWorker {
        worker, err := NewWorker(config)
        if err != nil {
            log.Fatal(err)
        }
        if err := worker.Start(); err != nil {
            log.Fatal(err)
        }
        waitSignal(worker.StopChan)
    } else {
        log.Fatalf("Unknown role: %s", *role)
    }
}

func waitSignal(stopChan chan struct{}) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    select {
    case <-sigChan:
        close(stopChan)
    }
}
