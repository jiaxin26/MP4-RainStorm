// rainstorm.go
package main

import (
    "bufio"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "net"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "time"
)

// RainStorm配置常量
const (
    LeaderPort = 9000  // Leader节点端口
    MaxRetries = 3     // 最大重试次数
    BatchSize  = 1000  // 处理批次大小
    LogPrefix  = "[RainStorm] "
)

// 操作类型
type OperationType int

const (
    Transform OperationType = iota
    FilteredTransform
    AggregateByKey
)

// 任务状态
type TaskStatus int

const (
    TaskPending TaskStatus = iota
    TaskRunning
    TaskCompleted
    TaskFailed
)

// 数据元组
type Tuple struct {
    Key   string      `json:"key"`
    Value interface{} `json:"value"`
}

// 任务信息
type TaskInfo struct {
    ID          string      `json:"id"`
    Type        string      `json:"type"`
    Status      TaskStatus  `json:"status"`
    WorkerID    string      `json:"worker_id"`
    InputFile   string      `json:"input_file"`
    OutputFile  string      `json:"output_file"`
    StartTime   time.Time   `json:"start_time"`
    LastUpdated time.Time   `json:"last_updated"`
}

// Worker状态
type WorkerState struct {
    ID            string    `json:"id"`
    Address       string    `json:"address"`
    Port          int       `json:"port"`
    LastHeartbeat time.Time `json:"last_heartbeat"`
    Tasks         []string  `json:"tasks"`
}

// RainStorm系统
type RainStorm struct {
    isLeader      bool
    leaderAddress string
    workerID      string
    config        *Config
    workers       map[string]*WorkerState
    tasks         map[string]*TaskInfo
    mutex         sync.RWMutex
    logger        *log.Logger
}

// 系统配置
type Config struct {
    Op1Executable    string
    Op2Executable    string
    SourceFile       string
    DestFile         string
    NumTasks         int
    WorkerPort       int
    HydfsNode       *Node  // 复用你的HyDFS Node结构
}

// 操作符接口
type Operator interface {
    Process(input []*Tuple) ([]*Tuple, error)
    GetState() ([]byte, error)
    RestoreState(state []byte) error
}

// 任务结果
type TaskResult struct {
    TaskID  string
    Success bool
    Error   error
    Data    []*Tuple
}

// 创建新的RainStorm实例
func NewRainStorm(config *Config, isLeader bool) (*RainStorm, error) {
    rs := &RainStorm{
        isLeader:      isLeader,
        leaderAddress: fmt.Sprintf("localhost:%d", LeaderPort),
        config:        config,
        workers:       make(map[string]*WorkerState),
        tasks:         make(map[string]*TaskInfo),
        logger:        log.New(os.Stdout, LogPrefix, log.LstdFlags),
    }

    // 如果是leader，初始化任务管理
    if isLeader {
        if err := rs.initializeLeader(); err != nil {
            return nil, fmt.Errorf("failed to initialize leader: %v", err)
        }
    }

    return rs, nil
}

// 初始化Leader节点
func (rs *RainStorm) initializeLeader() error {
    // 启动TCP监听
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", LeaderPort))
    if err != nil {
        return fmt.Errorf("failed to start leader listener: %v", err)
    }

    // 启动worker连接处理
    go rs.handleConnections(listener)

    // 启动心跳检测
    go rs.checkWorkerHeartbeats()

    rs.logger.Printf("Leader started on port %d", LeaderPort)
    return nil
}

// 处理Worker连接
func (rs *RainStorm) handleConnections(listener net.Listener) {
    for {
        conn, err := listener.Accept()
        if err != nil {
            rs.logger.Printf("Error accepting connection: %v", err)
            continue
        }
        go rs.handleWorkerConnection(conn)
    }
}

// 处理Worker连接
func (rs *RainStorm) handleWorkerConnection(conn net.Conn) {
    defer conn.Close()

    // 读取Worker注册信息
    decoder := json.NewDecoder(conn)
    var msg struct {
        Type     string `json:"type"`
        WorkerID string `json:"worker_id"`
        Port     int    `json:"port"`
    }

    if err := decoder.Decode(&msg); err != nil {
        rs.logger.Printf("Error decoding worker message: %v", err)
        return
    }

    // 注册Worker
    if msg.Type == "register" {
        rs.mutex.Lock()
        rs.workers[msg.WorkerID] = &WorkerState{
            ID:            msg.WorkerID,
            Address:       strings.Split(conn.RemoteAddr().String(), ":")[0],
            Port:          msg.Port,
            LastHeartbeat: time.Now(),
        }
        rs.mutex.Unlock()
        rs.logger.Printf("Registered worker %s at %s:%d", msg.WorkerID, rs.workers[msg.WorkerID].Address, msg.Port)
    }
}

// 检查Worker心跳
func (rs *RainStorm) checkWorkerHeartbeats() {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        rs.mutex.Lock()
        now := time.Now()
        for id, worker := range rs.workers {
            if now.Sub(worker.LastHeartbeat) > 15*time.Second {
                rs.logger.Printf("Worker %s missed heartbeat, removing", id)
                delete(rs.workers, id)
                rs.handleWorkerFailure(id)
            }
        }
        rs.mutex.Unlock()
    }
}

// 处理Worker失败
func (rs *RainStorm) handleWorkerFailure(workerID string) {
    rs.mutex.Lock()
    defer rs.mutex.Unlock()

    // 找出失败Worker的任务
    var failedTasks []*TaskInfo
    for _, task := range rs.tasks {
        if task.WorkerID == workerID {
            task.Status = TaskFailed
            failedTasks = append(failedTasks, task)
        }
    }

    // 重新调度失败的任务
    for _, task := range failedTasks {
        if err := rs.rescheduleTask(task); err != nil {
            rs.logger.Printf("Failed to reschedule task %s: %v", task.ID, err)
        }
    }
}

// 重新调度任务
func (rs *RainStorm) rescheduleTask(task *TaskInfo) error {
    // 选择一个可用的Worker
    var selectedWorker *WorkerState
    for _, worker := range rs.workers {
        if len(worker.Tasks) < rs.config.NumTasks {
            selectedWorker = worker
            break
        }
    }

    if selectedWorker == nil {
        return fmt.Errorf("no available workers")
    }

    // 更新任务信息
    task.WorkerID = selectedWorker.ID
    task.Status = TaskPending
    selectedWorker.Tasks = append(selectedWorker.Tasks, task.ID)

    // 通知新Worker执行任务
    return rs.assignTaskToWorker(task, selectedWorker)
}

// 分配任务给Worker
func (rs *RainStorm) assignTaskToWorker(task *TaskInfo, worker *WorkerState) error {
    // 准备任务分配消息
    msg := struct {
        Type     string    `json:"type"`
        Task     *TaskInfo `json:"task"`
        Op1Path  string    `json:"op1_path"`
        Op2Path  string    `json:"op2_path"`
    }{
        Type:    "assign_task",
        Task:    task,
        Op1Path: rs.config.Op1Executable,
        Op2Path: rs.config.Op2Executable,
    }

    // 连接Worker
    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", worker.Address, worker.Port))
    if err != nil {
        return fmt.Errorf("failed to connect to worker: %v", err)
    }
    defer conn.Close()

    // 发送任务
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to send task: %v", err)
    }

    return nil
}

// 执行操作符
func executeOperator(opPath string, input []*Tuple) ([]*Tuple, error) {
    // 将输入数据序列化为JSON
    inputJSON, err := json.Marshal(input)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal input: %v", err)
    }

    // 创建临时输入文件
    inputFile, err := ioutil.TempFile("", "rainstorm-input-*.json")
    if err != nil {
        return nil, fmt.Errorf("failed to create temp input file: %v", err)
    }
    defer os.Remove(inputFile.Name())
    
    if _, err := inputFile.Write(inputJSON); err != nil {
        return nil, fmt.Errorf("failed to write input file: %v", err)
    }
    inputFile.Close()

    // 创建临时输出文件
    outputFile, err := ioutil.TempFile("", "rainstorm-output-*.json")
    if err != nil {
        return nil, fmt.Errorf("failed to create temp output file: %v", err)
    }
    defer os.Remove(outputFile.Name())
    outputFile.Close()

    // 执行操作符程序
    cmd := exec.Command(opPath, inputFile.Name(), outputFile.Name())
    if err := cmd.Run(); err != nil {
        return nil, fmt.Errorf("failed to execute operator: %v", err)
    }

    // 读取输出
    output, err := ioutil.ReadFile(outputFile.Name())
    if err != nil {
        return nil, fmt.Errorf("failed to read output file: %v", err)
    }

    // 解析输出
    var results []*Tuple
    if err := json.Unmarshal(output, &results); err != nil {
        return nil, fmt.Errorf("failed to unmarshal output: %v", err)
    }

    return results, nil
}

// Worker节点处理逻辑
func (rs *RainStorm) runWorker() error {
    // 启动Worker服务器
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.config.WorkerPort))
    if err != nil {
        return fmt.Errorf("failed to start worker listener: %v", err)
    }

    // 注册到Leader
    if err := rs.registerWithLeader(); err != nil {
        return fmt.Errorf("failed to register with leader: %v", err)
    }

    // 启动心跳
    go rs.sendHeartbeats()

    // 处理任务请求
    for {
        conn, err := listener.Accept()
        if err != nil {
            rs.logger.Printf("Error accepting connection: %v", err)
            continue
        }
        go rs.handleTaskRequest(conn)
    }
}

// 注册到Leader
func (rs *RainStorm) registerWithLeader() error {
    conn, err := net.Dial("tcp", rs.leaderAddress)
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    defer conn.Close()

    // 发送注册消息
    msg := struct {
        Type     string `json:"type"`
        WorkerID string `json:"worker_id"`
        Port     int    `json:"port"`
    }{
        Type:     "register",
        WorkerID: rs.workerID,
        Port:     rs.config.WorkerPort,
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to send registration: %v", err)
    }

    return nil
}

// 发送心跳
func (rs *RainStorm) sendHeartbeats() {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        if err := rs.sendHeartbeat(); err != nil {
            rs.logger.Printf("Failed to send heartbeat: %v", err)
        }
    }
}

// 发送单个心跳
func (rs *RainStorm) sendHeartbeat() error {
    conn, err := net.Dial("tcp", rs.leaderAddress)
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    defer conn.Close()

    msg := struct {
        Type     string `json:"type"`
        WorkerID string `json:"worker_id"`
    }{
        Type:     "heartbeat",
        WorkerID: rs.workerID,
    }

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to send heartbeat: %v", err)
    }

    return nil
}

// 处理任务请求
func (rs *RainStorm) handleTaskRequest(conn net.Conn) {
    defer conn.Close()

    // 解析任务请求
    var msg struct {
        Type    string    `json:"type"`
        Task    *TaskInfo `json:"task"`
        Op1Path string    `json:"op1_path"`
        Op2Path string    `json:"op2_path"`
    }

    decoder := json.NewDecoder(conn)
    if err := decoder.Decode(&msg); err != nil {
        rs.logger.Printf("Error decoding task request: %v", err)
        return
    }

    // 执行任务
    if msg.Type == "assign_task" {
        go rs.executeTask(msg.Task, msg.Op1Path, msg.Op2Path)
    }
}

// 执行任务
func (rs *RainStorm) executeTask(task *TaskInfo, op1Path, op2Path string) {
    rs.logger.Printf("Starting task %s", task.ID)

    // 更新任务状态
    task.Status = TaskRunning
    task.StartTime = time.Now()

    // 创建日志文件
    logFile := filepath.Join("logs", fmt.Sprintf("task_%s.log", task.ID))
    if err := os.MkdirAll("logs", 0755); err != nil {
        rs.logger.Printf("Failed to create log directory: %v", err)
        return
    }

    logger := log.New(os.Stdout, fmt.Sprintf("[Task %s] ", task.ID), log.LstdFlags)
    
    // 从HyDFS读取输入数据
    input, err := rs.readFromHydfs(task.InputFile)
    if err != nil {
        logger.Printf("Failed to read input: %v", err)
        task.Status = TaskFailed
        return
    }

    // 执行第一阶段操作符
    output1, err := executeOperator(op1Path, input)
    if err != nil {
        logger.Printf("Failed to execute first operator: %v", err)
        task.Status = TaskFailed
        return
    }

    // 执行第二阶段操作符
    output2, err := executeOperator(op2Path, output1)
    if err != nil {
        logger.Printf("Failed to execute second operator: %v", err)
        task.Status = TaskFailed
        return
    }

    // 将结果写入HyDFS
    if err := rs.writeToHydfs(task.OutputFile, output2); err != nil {
        logger.Printf("Failed to write output: %v", err)
        task.Status = TaskFailed
        return
    }

    task.Status = TaskCompleted
    task.LastUpdated = time.Now()
    logger.Printf("Task completed successfully")
}

// 从HyDFS读取数据
func (rs *RainStorm) readFromHydfs(filename string) ([]*Tuple, error) {
    // 使用HyDFS节点读取文件
    tempFile, err := ioutil.TempFile("", "hydfs-input-*")
    if err != nil {
        return nil, fmt.Errorf("failed to create temp file: %v", err)
    }
    defer os.Remove(tempFile.Name())
    
    if err := rs.config.HydfsNode.GetFile("client1", filename, tempFile.Name()); err != nil {
        return nil, fmt.Errorf("failed to get file from HyDFS: %v", err)
    }

    // 读取文件内容
    data, err := ioutil.ReadFile(tempFile.Name())
    if err != nil {
        return nil, fmt.Errorf("failed to read temp file: %v", err)
    }

    // 将数据转换为元组
    var tuples []*Tuple
    scanner := bufio.NewScanner(strings.NewReader(string(data)))
    lineNum := 0
    for scanner.Scan() {
        lineNum++
        tuples = append(tuples, &Tuple{
            Key:   fmt.Sprintf("%s:%d", filename, lineNum),
            Value: scanner.Text(),
        })
    }

    return tuples, scanner.Err()
}

// 写入数据到HyDFS
func (rs *RainStorm) writeToHydfs(filename string, tuples []*Tuple) error {
    // 创建临时文件
    tempFile, err := ioutil.TempFile("", "hydfs-output-*")
    if err != nil {
        return fmt.Errorf("failed to create temp file: %v", err)
    }
    defer os.Remove(tempFile.Name())

    // 写入数据
    writer := bufio.NewWriter(tempFile)
    for _, tuple := range tuples {
        if _, err := fmt.Fprintf(writer, "%v\n", tuple.Value); err != nil {
            return fmt.Errorf("failed to write tuple: %v", err)
        }
    }
    
    if err := writer.Flush(); err != nil {
        return fmt.Errorf("failed to flush writer: %v", err)
    }
    
    if err := tempFile.Close(); err != nil {
        return fmt.Errorf("failed to close temp file: %v", err)
    }

    // 使用HyDFS节点写入文件
    if err := rs.config.HydfsNode.CreateFile("client1", tempFile.Name(), filename); err != nil {
        return fmt.Errorf("failed to create file in HyDFS: %v", err)
    }

    return nil
}

// 启动RainStorm系统
func (rs *RainStorm) Start() error {
    if rs.isLeader {
        return rs.runLeader()
    }
    return rs.runWorker()
}

// 运行Leader节点
func (rs *RainStorm) runLeader() error {
    // 验证输入文件存在
    if exists, err := rs.config.HydfsNode.FileExists(rs.config.SourceFile); !exists || err != nil {
        return fmt.Errorf("source file %s not found in HyDFS", rs.config.SourceFile)
    }

    // 初始化任务
    if err := rs.initializeTasks(); err != nil {
        return fmt.Errorf("failed to initialize tasks: %v", err)
    }

    // 等待所有任务完成
    return rs.waitForCompletion()
}

// 初始化任务
func (rs *RainStorm) initializeTasks() error {
    // 获取文件大小
    fileInfo, err := rs.config.HydfsNode.GetFileInfo(rs.config.SourceFile)
    if err != nil {
        return fmt.Errorf("failed to get file info: %v", err)
    }

    // 计算每个任务的数据范围
    chunkSize := fileInfo.Size / int64(rs.config.NumTasks)
    if chunkSize == 0 {
        chunkSize = 1
    }

    // 创建任务
    for i := 0; i < rs.config.NumTasks; i++ {
        taskID := fmt.Sprintf("task-%d", i)
        rs.tasks[taskID] = &TaskInfo{
            ID:         taskID,
            Status:     TaskPending,
            InputFile:  rs.config.SourceFile,
            OutputFile: fmt.Sprintf("%s.part%d", rs.config.DestFile, i),
        }
    }

    return nil
}

// 等待所有任务完成
func (rs *RainStorm) waitForCompletion() error {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    timeout := time.After(30 * time.Minute)

    for {
        select {
        case <-ticker.C:
            completed := 0
            failed := 0

            rs.mutex.RLock()
            for _, task := range rs.tasks {
                if task.Status == TaskCompleted {
                    completed++
                } else if task.Status == TaskFailed {
                    failed++
                }
            }
            rs.mutex.RUnlock()

            if completed == len(rs.tasks) {
                return rs.mergeFinalOutput()
            }

            if failed > 0 {
                return fmt.Errorf("%d tasks failed", failed)
            }

        case <-timeout:
            return fmt.Errorf("execution timed out")
        }
    }
}

// 合并最终输出
func (rs *RainStorm) mergeFinalOutput() error {
    // 创建临时文件
    tempFile, err := ioutil.TempFile("", "final-output-*")
    if err != nil {
        return fmt.Errorf("failed to create temp file: %v", err)
    }
    defer os.Remove(tempFile.Name())

    // 按顺序合并所有部分
    writer := bufio.NewWriter(tempFile)
    for i := 0; i < rs.config.NumTasks; i++ {
        partFile := fmt.Sprintf("%s.part%d", rs.config.DestFile, i)
        
        // 从HyDFS读取部分文件
        partTempFile, err := ioutil.TempFile("", "part-*")
        if err != nil {
            return fmt.Errorf("failed to create part temp file: %v", err)
        }
        defer os.Remove(partTempFile.Name())

        if err := rs.config.HydfsNode.GetFile("client1", partFile, partTempFile.Name()); err != nil {
            return fmt.Errorf("failed to get part file: %v", err)
        }

        // 复制内容
        data, err := ioutil.ReadFile(partTempFile.Name())
        if err != nil {
            return fmt.Errorf("failed to read part file: %v", err)
        }

        if _, err := writer.Write(data); err != nil {
            return fmt.Errorf("failed to write part data: %v", err)
        }
    }

    if err := writer.Flush(); err != nil {
        return fmt.Errorf("failed to flush final output: %v", err)
    }

    // 写入最终文件到HyDFS
    if err := rs.config.HydfsNode.CreateFile("client1", tempFile.Name(), rs.config.DestFile); err != nil {
        return fmt.Errorf("failed to create final output in HyDFS: %v", err)
    }

    return nil
}

func main() {
    if len(os.Args) != 6 {
        fmt.Printf("Usage: %s <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_file> <num_tasks>\n", os.Args[0])
        os.Exit(1)
    }

    // 解析命令行参数
    op1Exe := os.Args[1]
    op2Exe := os.Args[2]
    sourceFile := os.Args[3]
    destFile := os.Args[4]
    numTasks, err := strconv.Atoi(os.Args[5])
    if err != nil {
        fmt.Printf("Invalid number of tasks: %v\n", err)
        os.Exit(1)
    }

    // 验证可执行文件
    if _, err := exec.LookPath(op1Exe); err != nil {
        fmt.Printf("Operator 1 executable not found: %v\n", err)
        os.Exit(1)
    }
    if _, err := exec.LookPath(op2Exe); err != nil {
        fmt.Printf("Operator 2 executable not found: %v\n", err)
        os.Exit(1)
    }

    // 创建HyDFS节点
    hydfsNode, err := NewNode("rainstorm-client", "localhost", 9001)
    if err != nil {
        fmt.Printf("Failed to create HyDFS node: %v\n", err)
        os.Exit(1)
    }

    // 启动HyDFS节点
    if err := hydfsNode.Start(); err != nil {
        fmt.Printf("Failed to start HyDFS node: %v\n", err)
        os.Exit(1)
    }

    // 配置RainStorm
    config := &Config{
        Op1Executable: op1Exe,
        Op2Executable: op2Exe,
        SourceFile:    sourceFile,
        DestFile:      destFile,
        NumTasks:      numTasks,
        WorkerPort:    9002,
        HydfsNode:     hydfsNode,
    }

    // 创建并启动RainStorm Leader
    rs, err := NewRainStorm(config, true)
    if err != nil {
        fmt.Printf("Failed to create RainStorm: %v\n", err)
        os.Exit(1)
    }

    // 启动系统
    if err := rs.Start(); err != nil {
        fmt.Printf("RainStorm execution failed: %v\n", err)
        os.Exit(1)
    }

    fmt.Println("RainStorm execution completed successfully")
}
