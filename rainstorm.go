// rainstorm.go
package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "net"
    "os"
    "os/exec"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// 系统常量
const (
    LeaderPort = 8000
    WorkerBasePort = 8001
    MaxBatchSize = 1000
    BatchTimeout = 100 * time.Millisecond
    TaskTimeout = 5 * time.Second
    RetryInterval = 1 * time.Second
    MaxRetries = 3
    DefaultHyDFSPort = 5000
)

// 错误定义
var (
    ErrTaskNotFound = errors.New("task not found")
    ErrTaskFailed = errors.New("task execution failed")
    ErrInvalidState = errors.New("invalid task state")
    ErrDuplicateRecord = errors.New("duplicate record detected")
    ErrInvalidFormat = errors.New("invalid record format")
)

// 任务状态
type TaskState int

const (
    TaskStateInit TaskState = iota
    TaskStateRunning
    TaskStateCompleted
    TaskStateFailed
)

// 操作类型
type OperatorType int

const (
    OpTransform OperatorType = iota
    OpFilteredTransform
    OpAggregateByKey
)

// Message 结构体定义
type Message struct {
    Type      string      `json:"type"`
    SenderID  string      `json:"sender_id"` 
    Data      interface{} `json:"data"`
    Timestamp time.Time   `json:"timestamp"`
}

// Record 表示一条数据记录
type Record struct {
    Key       string    `json:"key"`
    Value     string    `json:"value"`
    UniqueID  string    `json:"unique_id"`
    Timestamp time.Time `json:"timestamp"`
}

// Task 表示一个处理任务
type Task struct {
    ID           string            `json:"id"`
    Type         OperatorType      `json:"type"`
    State        TaskState         `json:"state"`
    InputFiles   []string          `json:"input_files"`
    OutputFile   string           `json:"output_file"`
    Pattern      string           `json:"pattern"`
    ProcessedIDs map[string]bool   `json:"processed_ids"`
    StateData    map[string]int64  `json:"state_data"`
    StartTime    time.Time         `json:"start_time"`
    LastUpdate   time.Time         `json:"last_update"`
    mutex        sync.RWMutex
}

// Worker 表示一个工作节点
type Worker struct {
    ID        string
    Address   string
    Port      int
    Tasks     map[string]*Task
    HyDFS     *Node  // HyDFS节点实例
    Leader    string // Leader地址
    mutex     sync.RWMutex
    stopChan  chan struct{}
}

// Leader 表示主节点
type Leader struct {
    Workers       map[string]*Worker
    Tasks         map[string]*Task
    Assignments   map[string][]string
    HyDFS         *Node
    mutex         sync.RWMutex
    stopChan      chan struct{}
}

// 创建新的Leader
func NewLeader(hydfsNode *Node) *Leader {
    return &Leader{
        Workers:     make(map[string]*Worker),
        Tasks:       make(map[string]*Task),
        Assignments: make(map[string][]string),
        HyDFS:      hydfsNode,
        stopChan:   make(chan struct{}),
    }
}

// 创建新的Worker
func NewWorker(id, address string, port int, hydfsNode *Node, leaderAddr string) *Worker {
    return &Worker{
        ID:       id,
        Address:  address,
        Port:     port,
        Tasks:    make(map[string]*Task),
        HyDFS:    hydfsNode,
        Leader:   leaderAddr,
        stopChan: make(chan struct{}),
    }
}

// Leader方法
func (l *Leader) Start() error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", LeaderPort))
    if err != nil {
        return fmt.Errorf("failed to start leader: %v", err)
    }
    defer listener.Close()

    log.Printf("Leader started on port %d", LeaderPort)

    // 启动监控goroutine
    go l.monitorWorkers()

    // 主循环处理连接
    for {
        select {
        case <-l.stopChan:
            return nil
        default:
            conn, err := listener.Accept()
            if err != nil {
                log.Printf("Accept error: %v", err)
                continue
            }
            go l.handleConnection(conn)
        }
    }
}

func (l *Leader) handleConnection(conn net.Conn) {
    defer conn.Close()

    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        log.Printf("Failed to decode message: %v", err)
        return
    }

    var response Message
    switch msg.Type {
    case "REGISTER_WORKER":
        response = l.handleWorkerRegistration(msg)
    case "TASK_STATUS":
        response = l.handleTaskStatus(msg)
    case "TASK_COMPLETE":
        response = l.handleTaskComplete(msg)
    case "TASK_FAILED":
        response = l.handleTaskFailure(msg)
    default:
        response = Message{Type: "ERROR", Data: "unknown message type"}
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        log.Printf("Failed to send response: %v", err)
    }
}

func (l *Leader) handleWorkerRegistration(msg Message) Message {
    data := msg.Data.(map[string]interface{})
    workerID := data["worker_id"].(string)
    address := data["address"].(string)
    port := int(data["port"].(float64))

    l.mutex.Lock()
    l.Workers[workerID] = &Worker{
        ID:      workerID,
        Address: address,
        Port:    port,
        Tasks:   make(map[string]*Task),
    }
    l.mutex.Unlock()

    log.Printf("Registered new worker: %s at %s:%d", workerID, address, port)
    return Message{Type: "REGISTER_RESPONSE", Data: "OK"}
}

func (l *Leader) handleTaskStatus(msg Message) Message {
    data := msg.Data.(map[string]interface{})
    taskID := data["task_id"].(string)
    status := TaskState(data["status"].(float64))

    l.mutex.Lock()
    if task, exists := l.Tasks[taskID]; exists {
        task.State = status
        task.LastUpdate = time.Now()
        log.Printf("Updated task %s status to %v", taskID, status)
    }
    l.mutex.Unlock()

    return Message{Type: "STATUS_RESPONSE"}
}

func (l *Leader) handleTaskComplete(msg Message) Message {
    data := msg.Data.(map[string]interface{})
    taskID := data["task_id"].(string)

    l.mutex.Lock()
    if task, exists := l.Tasks[taskID]; exists {
        task.State = TaskStateCompleted
        task.LastUpdate = time.Now()
        log.Printf("Task %s completed", taskID)
    }
    l.mutex.Unlock()

    return Message{Type: "COMPLETE_RESPONSE"}
}

func (l *Leader) handleTaskFailure(msg Message) Message {
    data := msg.Data.(map[string]interface{})
    taskID := data["task_id"].(string)
    workerID := data["worker_id"].(string)

    l.mutex.Lock()
    if task, exists := l.Tasks[taskID]; exists {
        task.State = TaskStateFailed
        log.Printf("Task %s failed on worker %s", taskID, workerID)
        // 重新调度任务
        l.rescheduleTask(task, workerID)
    }
    l.mutex.Unlock()

    return Message{Type: "FAILURE_RESPONSE"}
}

func (l *Leader) monitorWorkers() {
    ticker := time.NewTicker(TaskTimeout / 2)
    defer ticker.Stop()

    for {
        select {
        case <-l.stopChan:
            return
        case <-ticker.C:
            l.checkWorkerHealth()
        }
    }
}

func (l *Leader) checkWorkerHealth() {
    l.mutex.Lock()
    defer l.mutex.Unlock()

    for workerID, worker := range l.Workers {
        if err := l.pingWorker(worker); err != nil {
            log.Printf("Worker %s failed health check: %v", workerID, err)
            l.handleWorkerFailure(workerID)
        }
    }
}

func (l *Leader) pingWorker(worker *Worker) error {
    conn, err := net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", worker.Address, worker.Port),
        time.Second)
    if err != nil {
        return err
    }
    defer conn.Close()

    msg := Message{Type: "PING"}
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return err
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return err
    }

    if response.Type != "PONG" {
        return errors.New("invalid ping response")
    }

    return nil
}

func (l *Leader) handleWorkerFailure(workerID string) {
    // 获取失败worker的任务
    tasks := l.Assignments[workerID]
    delete(l.Assignments, workerID)
    delete(l.Workers, workerID)

    // 重新调度任务
    for _, taskID := range tasks {
        if task, exists := l.Tasks[taskID]; exists {
            l.rescheduleTask(task, workerID)
        }
    }
}

func (l *Leader) rescheduleTask(task *Task, failedWorkerID string) {
    // 找到新的worker
    var newWorkerID string
    for id := range l.Workers {
        if id != failedWorkerID {
            newWorkerID = id
            break
        }
    }

    if newWorkerID == "" {
        log.Printf("No available workers to reschedule task %s", task.ID)
        return
    }

    worker := l.Workers[newWorkerID]
    if err := l.assignTaskToWorker(task, worker); err != nil {
        log.Printf("Failed to reschedule task %s: %v", task.ID, err)
    }
}

func (l *Leader) assignTaskToWorker(task *Task, worker *Worker) error {
    msg := Message{
        Type: "ASSIGN_TASK",
        Data: map[string]interface{}{
            "task_id":     task.ID,
            "type":        task.Type,
            "pattern":     task.Pattern,
            "input_files": task.InputFiles,
            "output_file": task.OutputFile,
        },
    }

    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", worker.Address, worker.Port))
    if err != nil {
        return err
    }
    defer conn.Close()

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return err
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return err
    }

    if response.Type == "ERROR" {
        return fmt.Errorf("task assignment failed: %v", response.Data)
    }

    l.Assignments[worker.ID] = append(l.Assignments[worker.ID], task.ID)
    return nil
}

// Worker方法
func (w *Worker) Start() error {
    // 注册到Leader
    if err := w.registerWithLeader(); err != nil {
        return err
    }

    // 启动任务处理服务
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", w.Port))
    if err != nil {
        return err
    }
    defer listener.Close()

    log.Printf("Worker started on port %d", w.Port)

    for {
        select {
        case <-w.stopChan:
            return nil
        default:
            conn, err := listener.Accept()
            if err != nil {
                log.Printf("Accept error: %v", err)
                continue
            }
            go w.handleConnection(conn)
        }
    }
}

func (w *Worker) registerWithLeader() error {
    conn, err := net.Dial("tcp", w.Leader)
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    defer conn.Close()

    msg := Message{
        Type: "REGISTER_WORKER",
        Data: map[string]interface{}{
            "worker_id": w.ID,
            "address":   w.Address,
            "port":     w.Port,
        },
        Timestamp: time.Now(),
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to register: %v", err)
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return fmt.Errorf("failed to receive response: %v", err)
    }

    if response.Type != "REGISTER_RESPONSE" {
        return fmt.Errorf("unexpected response: %s", response.Type)
    }

    log.Printf("Successfully registered with leader")
    return nil
}

func (w *Worker) handleConnection(conn net.Conn) {
    defer conn.Close()

    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        log.Printf("Failed to decode message: %v", err)
        return
    }

    var response Message
    switch msg.Type {
    case "PING":
        response = Message{Type: "PONG"}
    case "ASSIGN_TASK":
        response = w.handleTaskAssignment(msg)
    default:
        response = Message{Type: "ERROR", Data: "unknown message type"}
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        log.Printf("Failed to send response: %v", err)
    }
}

func (w *Worker) handleTaskAssignment(msg Message) Message {
    data := msg.Data.(map[string]interface{})
    task := &Task{
        ID:          data["task_id"].(string),
        Type:        OperatorType(data["type"].(float64)),
        Pattern:     data["pattern"].(string),
        State:       TaskStateInit,
        StartTime:   time.Now(),
        LastUpdate:  time.Now(),
        ProcessedIDs: make(map[string]bool),
        StateData:    make(map[string]int64),
    }

    // 获取输入文件列表
    inputFiles := data["input_files"].([]interface{})
    task.InputFiles = make([]string, len(inputFiles))
    for i, f := range inputFiles {
        task.InputFiles[i] = f.(string)
    }
    task.OutputFile = data["output_file"].(string)

    w.mutex.Lock()
    w.Tasks[task.ID] = task
    w.mutex.Unlock()

    // 异步执行任务
    go w.executeTask(task)

    return Message{Type: "ASSIGN_RESPONSE", Data: "OK"}
}

func (w *Worker) executeTask(task *Task) {
    log.Printf("Starting task %s", task.ID)

    // 更新任务状态
    task.mutex.Lock()
    task.State = TaskStateRunning
    task.mutex.Unlock()

    // 读取和处理输入
    records, err := w.readInput(task)
    if err != nil {
        w.handleTaskError(task, err)
        return
    }

    // 根据任务类型处理记录
    var results []Record
    switch task.Type {
    case OpTransform:
        results, err = w.processFilterTask(task, records)
    case OpAggregateByKey:
        results, err = w.processCountTask(task, records)
    }

    if err != nil {
        w.handleTaskError(task, err)
        return
    }

    // 写入结果
    if err := w.writeResults(task, results); err != nil {
        w.handleTaskError(task, err)
        return
    }

    // 完成任务
    w.completeTask(task)
}

func (w *Worker) readInput(task *Task) ([]Record, error) {
    var records []Record

    for _, inputFile := range task.InputFiles {
        // 创建临时文件
        tempFile := fmt.Sprintf("/tmp/%s_%s", task.ID, filepath.Base(inputFile))
        
        // 从HyDFS读取文件
        if err := w.HyDFS.GetFile(w.ID, inputFile, tempFile); err != nil {
            return nil, fmt.Errorf("failed to get file from HyDFS: %v", err)
        }

        // 读取记录
        file, err := os.Open(tempFile)
        if err != nil {
            os.Remove(tempFile)
            return nil, fmt.Errorf("failed to open temp file: %v", err)
        }

        scanner := bufio.NewScanner(file)
        lineNum := 0
        // 跳过标题行
        if scanner.Scan() {
            lineNum++
        }

        for scanner.Scan() {
            lineNum++
            line := scanner.Text()
            record := Record{
                Key:       fmt.Sprintf("%s:%d", inputFile, lineNum),
                Value:     line,
                UniqueID:  fmt.Sprintf("%s_%d", inputFile, lineNum),
                Timestamp: time.Now(),
            }
            records = append(records, record)
        }

        file.Close()
        os.Remove(tempFile)

        if err := scanner.Err(); err != nil {
            return nil, fmt.Errorf("error reading file: %v", err)
        }
    }

    return records, nil
}

func (w *Worker) processFilterTask(task *Task, records []Record) ([]Record, error) {
    var results []Record
    for _, record := range records {
        if task.ProcessedIDs[record.UniqueID] {
            continue // 跳过重复记录
        }

        // 拆分CSV行
        fields := strings.Split(record.Value, ",")
        if !strings.Contains(record.Value, task.Pattern) {
            continue
        }

        // 查找OBJECTID和Sign_Type列
        var objectID, signType string
        for _, field := range fields {
            field = strings.TrimSpace(field)
            if strings.Contains(strings.ToLower(field), "objectid") {
                objectID = field
            } else if strings.Contains(strings.ToLower(field), "sign_type") {
                signType = field
            }
        }

        if objectID != "" && signType != "" {
            results = append(results, Record{
                Key:       objectID,
                Value:     signType,
                UniqueID:  record.UniqueID + "_filtered",
                Timestamp: time.Now(),
            })
        }

        task.ProcessedIDs[record.UniqueID] = true
    }
    return results, nil
}

func (w *Worker) processCountTask(task *Task, records []Record) ([]Record, error) {
    categoryCount := make(map[string]int64)

    for _, record := range records {
        if task.ProcessedIDs[record.UniqueID] {
            continue
        }

        fields := strings.Split(record.Value, ",")
        var signPost, category string
        for _, field := range fields {
            field = strings.TrimSpace(field)
            if strings.Contains(strings.ToLower(field), "sign post") {
                signPost = field
            } else if strings.Contains(strings.ToLower(field), "category") {
                category = field
            }
        }

        // 检查是否匹配指定的Sign Post类型
        if signPost == task.Pattern {
            categoryCount[category]++
            task.ProcessedIDs[record.UniqueID] = true
        }
    }

    // 转换结果
    var results []Record
    for category, count := range categoryCount {
        results = append(results, Record{
            Key:       category,
            Value:     strconv.FormatInt(count, 10),
            UniqueID:  fmt.Sprintf("%s_count_%d", category, count),
            Timestamp: time.Now(),
        })
    }

    return results, nil
}

func (w *Worker) writeResults(task *Task, results []Record) error {
    if len(results) == 0 {
        return nil
    }

    // 准备输出数据
    var buffer bytes.Buffer
    for _, record := range results {
        fmt.Fprintf(&buffer, "%s\t%s\n", record.Key, record.Value)
    }

    // 创建临时文件
    tempFile := fmt.Sprintf("/tmp/%s_output", task.ID)
    if err := ioutil.WriteFile(tempFile, buffer.Bytes(), 0644); err != nil {
        return fmt.Errorf("failed to write temp file: %v", err)
    }
    defer os.Remove(tempFile)

    // 追加到HyDFS输出文件
    return w.HyDFS.AppendFile(w.ID, tempFile, task.OutputFile)
}

func (w *Worker) completeTask(task *Task) {
    task.mutex.Lock()
    task.State = TaskStateCompleted
    task.LastUpdate = time.Now()
    task.mutex.Unlock()

    log.Printf("Task %s completed successfully", task.ID)

    // 通知Leader任务完成
    w.notifyTaskComplete(task)
}

func (w *Worker) handleTaskError(task *Task, err error) {
    log.Printf("Task %s failed: %v", task.ID, err)
    
    task.mutex.Lock()
    task.State = TaskStateFailed
    task.LastUpdate = time.Now()
    task.mutex.Unlock()

    // 通知Leader任务失败
    w.notifyTaskFailed(task)
}

func (w *Worker) notifyTaskComplete(task *Task) {
    msg := Message{
        Type: "TASK_COMPLETE",
        Data: map[string]interface{}{
            "task_id":   task.ID,
            "worker_id": w.ID,
        },
        Timestamp: time.Now(),
    }
    w.sendToLeader(msg)
}

func (w *Worker) notifyTaskFailed(task *Task) {
    msg := Message{
        Type: "TASK_FAILED",
        Data: map[string]interface{}{
            "task_id":   task.ID,
            "worker_id": w.ID,
        },
        Timestamp: time.Now(),
    }
    w.sendToLeader(msg)
}

func (w *Worker) sendToLeader(msg Message) {
    conn, err := net.Dial("tcp", w.Leader)
    if err != nil {
        log.Printf("Failed to connect to leader: %v", err)
        return
    }
    defer conn.Close()

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        log.Printf("Failed to send message to leader: %v", err)
        return
    }
}

func main() {
    if len(os.Args) < 7 {
        fmt.Println("Usage: rainstorm <str1> <str2> <hydfs_src_file> <hydfs_dest_file> <num_tasks> <role>")
        os.Exit(1)
    }

    pattern1 := os.Args[1]    // 匹配模式1
    signPostType := os.Args[2] // Sign Post类型
    inputFile := os.Args[3]    // 输入文件
    outputFile := os.Args[4]   // 输出文件
    numTasks, _ := strconv.Atoi(os.Args[5])
    role := os.Args[6]

    // 获取主机名作为节点ID
    hostname, err := os.Hostname()
    if err != nil {
        log.Fatalf("Failed to get hostname: %v", err)
    }

    // 初始化HyDFS节点
    isIntroducer := role == "leader"
    hydfsNode, err := initHydfs(hostname, hostname, DefaultHyDFSPort, isIntroducer)
    if err != nil {
        log.Fatalf("Failed to initialize HyDFS: %v", err)
    }

    switch role {
    case "leader":
        leader := NewLeader(hydfsNode)
        // 创建初始任务
        task1 := &Task{
            ID:         "filter_task",
            Type:      OpTransform,
            Pattern:   pattern1,
            InputFiles: []string{inputFile},
            OutputFile: outputFile + "_app1",
        }
        task2 := &Task{
            ID:         "count_task",
            Type:      OpAggregateByKey,
            Pattern:   signPostType,
            InputFiles: []string{inputFile},
            OutputFile: outputFile + "_app2",
        }
        
        // 添加任务到leader
        leader.Tasks[task1.ID] = task1
        leader.Tasks[task2.ID] = task2

        // 设置任务数量
        for i := 0; i < numTasks; i++ {
            // 这里可以添加任务分配逻辑
        }

        if err := leader.Start(); err != nil {
            log.Fatalf("Leader failed: %v", err)
        }

    case "worker":
        worker := NewWorker(
            hostname,
            hostname,
            WorkerBasePort,
            hydfsNode,
            fmt.Sprintf("localhost:%d", LeaderPort),
        )
        if err := worker.Start(); err != nil {
            log.Fatalf("Worker failed: %v", err)
        }

    default:
        log.Fatalf("Unknown role: %s", role)
    }
}
