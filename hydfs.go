// hydfs.go
package main

import (
    "bufio"
    "bytes"
    "container/list"
    "crypto/sha256"
    "encoding/base64"
    "encoding/binary"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "io/ioutil"
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

// 系统常量
const (
    // 文件系统配置
    ReplicaCount         = 3           // 副本数量
    VirtualNodesPerNode  = 10          // 每个节点的虚拟节点数
    MaxFileSize          = 100 << 20   // 最大文件大小: 100MB
    BlockSize            = 4 << 20     // 块大小: 4MB
    BasePort            = 5000         // 基础端口号
    
    // 缓存配置
    MaxCacheSize        = 1 << 30     // 最大缓存大小: 1GB
    CacheExpirationTime = 5 * time.Minute
    
    // 超时配置
    ConnectTimeout      = 5 * time.Second
    OperationTimeout    = 5 * time.Second
    MergeTimeout        = 10 * time.Second
    ReplicationTimeout  = 15 * time.Second
    HeartbeatInterval   = 2 * time.Second
    SuspicionTimeout    = 6 * time.Second
    FailureTimeout      = 10 * time.Second
)

// 错误定义
var (
    ErrFileNotFound     = errors.New("file not found")
    ErrFileExists       = errors.New("file already exists")
    ErrNoQuorum        = errors.New("failed to achieve quorum")
    ErrInvalidVersion  = errors.New("invalid version")
    ErrNodeNotFound    = errors.New("node not found")
    ErrTimeout         = errors.New("operation timed out")
    ErrInvalidOperation = errors.New("invalid operation")
)

// 操作类型
type OperationType int

const (
    OpNone OperationType = iota
    OpCreate
    OpAppend
    OpDelete
    OpMerge
)

// 节点状态
type NodeStatus int

const (
    StatusNormal NodeStatus = iota
    StatusSuspected
    StatusFailed
)

// Block 文件块信息
type Block struct {
    ID          int       `json:"id"`
    Size        int       `json:"size"`
    Checksum    []byte    `json:"checksum"`
    Data        []byte    `json:"data,omitempty"`
}

type ReplicationData struct {
    Filename string    `json:"filename"`
    Info     FileInfo  `json:"info"`
    Data     string    `json:"data"`      // Base64编码的文件数据
    Checksum string    `json:"checksum"`  // Base64编码的校验和
}

const (
    MessageTypeGetOplog       = "GET_OPLOG"
    MessageTypeGetOplogResponse = "GET_OPLOG_RESPONSE"
)


// Operation 操作记录
type Operation struct {
    Type      OperationType `json:"type"`
    ClientID  string        `json:"client_id"`
    Filename  string        `json:"filename"`
    Timestamp time.Time     `json:"timestamp"`
    Data      []byte        `json:"data,omitempty"`
    SeqNum    int64         `json:"seq_num"`
    Checksum  []byte        `json:"checksum"`
    Version   int           `json:"version"`
}


func (op *Operation) UnmarshalJSON(data []byte) error {
    var aux struct {
        Type      OperationType `json:"type"`
        ClientID  string        `json:"client_id"`
        Filename  string        `json:"filename"`
        Timestamp string        `json:"timestamp"`
        Data      string        `json:"data,omitempty"`     // Base64 编码的字符串
        SeqNum    int64         `json:"seq_num"`
        Checksum  string        `json:"checksum"`           // Base64 编码的字符串
        Version   int           `json:"version"`
    }

    if err := json.Unmarshal(data, &aux); err != nil {
        return err
    }

    op.Type = aux.Type
    op.ClientID = aux.ClientID
    op.Filename = aux.Filename

    // 解析时间戳
    parsedTime, err := time.Parse(time.RFC3339, aux.Timestamp)
    if err != nil {
        return fmt.Errorf("invalid timestamp format: %v", err)
    }
    op.Timestamp = parsedTime

    // 解码 Data 字段
    if aux.Data != "" {
        decodedData, err := base64.StdEncoding.DecodeString(aux.Data)
        if err != nil {
            return fmt.Errorf("failed to decode Data field: %v", err)
        }
        op.Data = decodedData
    }

    op.SeqNum = aux.SeqNum
    op.Version = aux.Version

    // 解码 Checksum 字段
    if aux.Checksum != "" {
        decodedChecksum, err := base64.StdEncoding.DecodeString(aux.Checksum)
        if err != nil {
            return fmt.Errorf("failed to decode Checksum field: %v", err)
        }
        op.Checksum = decodedChecksum
    }

    return nil
}

// FileInfo 文件信息
type FileInfo struct {
    Filename     string    `json:"filename"`
    Version      int       `json:"version"`
    Size         int64     `json:"size"`
    Blocks       []Block   `json:"blocks"`
    Checksum     []byte    `json:"checksum"`
    CreatedAt    time.Time `json:"created_at"`
    UpdatedAt    time.Time `json:"updated_at"`
    ReplicaNodes []string  `json:"replica_nodes"`
    mutex        sync.RWMutex
}

// type Message struct {
//     Type      string      `json:"type"`
//     SenderID  string      `json:"sender_id"`
//     Data      interface{} `json:"data"`
//     Timestamp time.Time   `json:"timestamp"`
// }

func (m Message) String() string {
    return fmt.Sprintf("Message{Type: %s, SenderID: %s, Timestamp: %s, DataType: %T}",
        m.Type, m.SenderID, m.Timestamp.Format(time.RFC3339), m.Data)
}

// MemberInfo 成员信息
type MemberInfo struct {
    ID            string
    Address       string
    Port          int
    Status        NodeStatus
    LastHeartbeat time.Time
}

// ClientCache 客户端缓存
type ClientCache struct {
    entries      map[string]*CacheEntry
    lru          *list.List
    mutex        sync.RWMutex
    maxSize      int64
    currentSize  int64
}

// CacheEntry 缓存条目
type CacheEntry struct {
    Data         []byte
    FileInfo     *FileInfo
    LastAccess   time.Time
    Size         int64
}

// Node 节点信息
type Node struct {
    ID              string
    Address         string
    Port            int
    Status          NodeStatus
    
    Files           map[string]*FileInfo   // 文件信息映射
    Blocks          map[string]map[int][]byte  // 文件块数据
    Cache           *ClientCache
    
    Members         map[string]*MemberInfo  // 成员列表
    Ring            *ConsistentHash         // 一致性哈希环
    
    OpLog           []Operation             // 操作日志
    LastSeqNum      map[string]int64       // 最后序列号
    
    suspicionEnabled bool
    suspicions       map[string]time.Time
    
    mutex           sync.RWMutex
    memberMutex     sync.RWMutex
    fileMutex       sync.RWMutex
    
    msgChan         chan Message
    cmdChan         chan string
    stopChan        chan struct{}

    mergeOplogChannel chan []Operation
    
    logger          *log.Logger
}

// ConsistentHash 一致性哈希环
type ConsistentHash struct {
    ring          map[uint32]string
    sortedHashes  []uint32
    vnodes        int
    mutex         sync.RWMutex
}

// 节点创建和初始化
func NewNode(id, address string, port int) (*Node, error) {
    node := &Node{
        ID:              id,
        Address:         address,
        Port:           port,
        Status:         StatusNormal,
        
        Files:          make(map[string]*FileInfo),
        Blocks:         make(map[string]map[int][]byte),
        Cache:          NewClientCache(MaxCacheSize),
        
        Members:        make(map[string]*MemberInfo),
        Ring:          NewConsistentHash(VirtualNodesPerNode),
        
        OpLog:          make([]Operation, 0),
        LastSeqNum:     make(map[string]int64),
        
        suspicionEnabled: false,
        suspicions:      make(map[string]time.Time),
        
        msgChan:        make(chan Message, 1000),
        cmdChan:        make(chan string, 100),
        stopChan:       make(chan struct{}),
        
        logger:         log.New(os.Stdout, fmt.Sprintf("[Node %s] ", id), log.LstdFlags),
        mergeOplogChannel: make(chan []Operation, 10),
    }

    // 创建数据目录
    if err := os.MkdirAll(filepath.Join("data", id), 0755); err != nil {
        return nil, fmt.Errorf("failed to create data directory: %v", err)
    }

    // 添加自己到成员列表和哈希环
    node.Members[id] = &MemberInfo{
        ID:            id,
        Address:       address,
        Port:          port,
        Status:        StatusNormal,
        LastHeartbeat: time.Now(),
    }
    node.Ring.AddNode(id)

    return node, nil
}

// 缓存实现
func NewClientCache(maxSize int64) *ClientCache {
    return &ClientCache{
        entries:     make(map[string]*CacheEntry),
        lru:         list.New(),
        maxSize:     maxSize,
        currentSize: 0,
    }
}

func (c *ClientCache) Get(key string) *CacheEntry {
    c.mutex.RLock()
    defer c.mutex.RUnlock()

    if entry, exists := c.entries[key]; exists {
        entry.LastAccess = time.Now()
        for e := c.lru.Front(); e != nil; e = e.Next() {
            if e.Value.(string) == key {
                c.lru.MoveToFront(e)
                break
            }
        }
        return entry
    }
    return nil
}

func (c *ClientCache) Put(key string, data []byte, info *FileInfo) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    size := int64(len(data))

    // 如果超过最大大小，删除旧条目
    for c.currentSize+size > c.maxSize && c.lru.Len() > 0 {
        if back := c.lru.Back(); back != nil {
            oldKey := back.Value.(string)
            c.removeEntry(oldKey)
        }
    }

    // 添加新条目
    if size <= c.maxSize {
        entry := &CacheEntry{
            Data:       data,
            FileInfo:   info,
            LastAccess: time.Now(),
            Size:       size,
        }
        c.entries[key] = entry
        c.lru.PushFront(key)
        c.currentSize += size
    }
}

func (c *ClientCache) removeEntry(key string) {
    if entry, exists := c.entries[key]; exists {
        c.currentSize -= entry.Size
        delete(c.entries, key)
        for e := c.lru.Front(); e != nil; e = e.Next() {
            if e.Value.(string) == key {
                c.lru.Remove(e)
                break
            }
        }
    }
}

func (c *ClientCache) Cleanup() {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    now := time.Now()
    for key, entry := range c.entries {
        if now.Sub(entry.LastAccess) > CacheExpirationTime {
            c.removeEntry(key)
        }
    }
}

// 文件信息相关方法
func (n *Node) GetFileInfo(sdfsPath string) (*FileInfo, error) {
    n.mutex.RLock()
    defer n.mutex.RUnlock()

    info, exists := n.Files[sdfsPath]
    if !exists {
        return nil, ErrFileNotFound
    }

    return info, nil
}

// 节点管理相关方法
func (n *Node) StartIntroducer() error {
    n.logger.Printf("Starting as introducer node")
    return nil
}


// func (n *Node) JoinCluster() error {
//     // 使用硬编码的默认introducer地址和端口
//     introducerAddr := "fa24-cs425-8101.cs.illinois.edu:9001"
    
//     // 如果环境变量设置了，则使用环境变量中的地址和端口
//     if host := os.Getenv("INTRODUCER_HOST"); host != "" {
//         if port := os.Getenv("INTRODUCER_PORT"); port != "" {
//             introducerAddr = fmt.Sprintf("%s:%s", host, port)
//         }
//     }

//     n.logger.Printf("Attempting to join cluster via introducer at %s", introducerAddr)
    
//     conn, err := net.DialTimeout("tcp", introducerAddr, ConnectTimeout)
//     if err != nil {
//         return fmt.Errorf("failed to connect to introducer: %v", err)
//     }
//     defer conn.Close()

//     // 发送加入请求
//     joinMsg := Message{
//         Type:      "JOIN",
//         SenderID:  n.ID,
//         Timestamp: time.Now(),
//         Data: map[string]interface{}{
//             "address": n.Address,
//             "port":    n.Port,
//         },
//     }

//     encoder := json.NewEncoder(conn)
//     if err := encoder.Encode(joinMsg); err != nil {
//         return fmt.Errorf("failed to send join request: %v", err)
//     }

//     // 等待响应
//     decoder := json.NewDecoder(conn)
//     var response Message
//     if err := decoder.Decode(&response); err != nil {
//         return fmt.Errorf("failed to receive join response: %v", err)
//     }

//     if response.Type == "ERROR" {
//         return fmt.Errorf("join request rejected: %v", response.Data)
//     }

//     // 更新成员列表
//     if memberList, ok := response.Data.(map[string]*MemberInfo); ok {
//         n.memberMutex.Lock()
//         for id, info := range memberList {
//             if id != n.ID {
//                 n.Members[id] = info
//                 n.Ring.AddNode(id)
//             }
//         }
//         n.memberMutex.Unlock()
//     }

//     n.logger.Printf("Successfully joined the cluster")
//     return nil
// }

func (n *Node) JoinCluster() error {
    introducerAddr := "fa24-cs425-8101.cs.illinois.edu:9001"
    
    if host := os.Getenv("INTRODUCER_HOST"); host != "" {
        if port := os.Getenv("INTRODUCER_PORT"); port != "" {
            introducerAddr = fmt.Sprintf("%s:%s", host, port)
        }
    }

    n.logger.Printf("Attempting to join cluster via introducer at %s", introducerAddr)
    
    // 尝试建立TCP连接
    conn, err := net.DialTimeout("tcp", introducerAddr, ConnectTimeout)
    if err != nil {
        return fmt.Errorf("failed to connect to introducer: %v", err)
    }
    defer conn.Close()

    // 准备加入请求
    joinMsg := Message{
        Type:      "JOIN",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data: map[string]interface{}{
            "address": n.Address,
            "port":    n.Port,
        },
    }

    // 发送请求
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(joinMsg); err != nil {
        return fmt.Errorf("failed to send join request: %v", err)
    }

    // 接收响应
    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return fmt.Errorf("failed to receive join response: %v", err)
    }

    if response.Type == "ERROR" {
        return fmt.Errorf("join request rejected: %v", response.Data)
    }

    // 更新成员列表
    if memberList, ok := response.Data.(map[string]interface{}); ok {
        n.memberMutex.Lock()
        for id, infoData := range memberList {
            if id != n.ID {
                if memberData, ok := infoData.(map[string]interface{}); ok {
                    address, _ := memberData["address"].(string)
                    port, _ := memberData["port"].(float64)
                    status, _ := memberData["status"].(float64)
                    
                    member := &MemberInfo{
                        ID:            id,
                        Address:       address,
                        Port:          int(port),
                        Status:        NodeStatus(status),
                        LastHeartbeat: time.Now(),
                    }
                    n.Members[id] = member
                    n.Ring.AddNode(id)
                    n.logger.Printf("Added member %s at %s:%d", id, address, int(port))
                }
            }
        }
        n.memberMutex.Unlock()
    }

    // 验证成员列表是否正确更新
    n.memberMutex.RLock()
    memberCount := len(n.Members)
    n.memberMutex.RUnlock()

    if memberCount < 2 {  // 至少应该包含自己和引导节点
        return fmt.Errorf("failed to get complete membership list, only got %d members", memberCount)
    }

    n.logger.Printf("Successfully joined the cluster with %d members", memberCount)
    return nil
}

func (n *Node) Stop() {
    n.logger.Printf("Stopping node %s", n.ID)
    
    // 发送离开消息
    leaveMsg := Message{
        Type:      "LEAVE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
    }
    n.broadcastMessage(leaveMsg, nil)

    // 关闭所有通道
    close(n.stopChan)
    close(n.msgChan)
    close(n.cmdChan)

    // 等待后台任务完成
    time.Sleep(time.Second)

    n.logger.Printf("Node stopped")
}

// 消息处理相关方法
func (n *Node) messageHandler() {
    for {
        select {
        case <-n.stopChan:
            return
        case msg := <-n.msgChan:
            n.handleMessage(msg)
        }
    }
}

func (n *Node) cleanupLoop() {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            n.Cache.Cleanup()
            n.cleanupOperationLog()
        }
    }
}

func (n *Node) cleanupOperationLog() {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    // 保留最近1000条操作记录
    const maxLogSize = 1000
    if len(n.OpLog) > maxLogSize {
        n.OpLog = n.OpLog[len(n.OpLog)-maxLogSize:]
    }
}

func (n *Node) replicationMonitor() {
    ticker := time.NewTicker(time.Minute * 5)
    defer ticker.Stop()

    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            n.checkReplication()
        }
    }
}

func (n *Node) checkReplication() {
    n.mutex.RLock()
    filesToCheck := make(map[string]*FileInfo)
    for filename, info := range n.Files {
        filesToCheck[filename] = info
    }
    n.mutex.RUnlock()

    for filename, info := range filesToCheck {
        // 检查每个副本节点的状态
        needsReplication := false
        var activeNodes []string

        n.memberMutex.RLock()
        for _, nodeID := range info.ReplicaNodes {
            if member, exists := n.Members[nodeID]; exists && member.Status == StatusNormal {
                activeNodes = append(activeNodes, nodeID)
            }
        }
        n.memberMutex.RUnlock()

        // 如果活跃副本数量小于要求，启动复制
        if len(activeNodes) < ReplicaCount {
            needsReplication = true
        }

        if needsReplication {
            n.replicateFileAfterFailure(filename, info, "")
        }
    }
}

// 成员管理相关方法
// func (n *Node) handleJoin(msg Message) error {
//     data, ok := msg.Data.(map[string]interface{})
//     if !ok {
//         return errors.New("invalid join message format")
//     }

//     address, ok := data["address"].(string)
//     if !ok {
//         return errors.New("invalid address in join message")
//     }

//     port, ok := data["port"].(float64)
//     if !ok {
//         return errors.New("invalid port in join message")
//     }

//     // 添加新成员
//     n.memberMutex.Lock()
//     n.Members[msg.SenderID] = &MemberInfo{
//         ID:            msg.SenderID,
//         Address:       address,
//         Port:         int(port),
//         Status:       StatusNormal,
//         LastHeartbeat: time.Now(),
//     }
//     n.Ring.AddNode(msg.SenderID)
//     n.memberMutex.Unlock()

//     // 广播新成员加入消息
//     joinNotification := Message{
//         Type:      "NEW_MEMBER",
//         SenderID:  n.ID,
//         Timestamp: time.Now(),
//         Data:      msg.Data,
//     }
//     n.broadcastMessage(joinNotification, []string{msg.SenderID})

//     return nil
// }

func (n *Node) handleJoin(msg Message) error {
    data, ok := msg.Data.(map[string]interface{})
    if !ok {
        return errors.New("invalid join message format")
    }

    address, ok := data["address"].(string)
    if !ok {
        return errors.New("invalid address in join message")
    }

    portFloat, ok := data["port"].(float64)
    if !ok {
        return errors.New("invalid port in join message")
    }
    port := int(portFloat)

    // 添加新成员
    n.memberMutex.Lock()
    n.Members[msg.SenderID] = &MemberInfo{
        ID:            msg.SenderID,
        Address:       address,
        Port:         port,
        Status:       StatusNormal,
        LastHeartbeat: time.Now(),
    }
    n.Ring.AddNode(msg.SenderID)
    
    // 准备完整的成员列表响应
    memberList := make(map[string]interface{})
    for id, member := range n.Members {
        memberList[id] = map[string]interface{}{
            "address": member.Address,
            "port":    member.Port,
            "status": int(member.Status),
        }
    }
    n.memberMutex.Unlock()

    // 广播新成员加入消息
    joinNotification := Message{
        Type:      "NEW_MEMBER",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      memberList,
    }
    n.broadcastMessage(joinNotification, []string{msg.SenderID})

    return nil
}


func (n *Node) handleLeave(msg Message) error {
    n.memberMutex.Lock()
    delete(n.Members, msg.SenderID)
    n.Ring.RemoveNode(msg.SenderID)
    n.memberMutex.Unlock()

    // 处理节点离开后的文件复制
    n.handleFailedNodeFiles(msg.SenderID)

    return nil
}

// 命令处理相关方法
func handleMultiAppend(n *Node, sdfsPath string, args []string) {
    nodeAppends := make(map[string]string)
    for i := 0; i < len(args); i += 2 {
        nodeAppends[args[i]] = args[i+1]
    }

    err := n.MultiAppend(sdfsPath, nodeAppends)
    if err != nil {
        fmt.Printf("Multi-append failed: %v\n", err)
    } else {
        fmt.Println("Successfully completed multi-append operation")
    }
}

func (n *Node) GetFileFromReplica(vmAddr, sdfsPath, localPath string) error {
    n.logger.Printf("Getting file %s from replica %s to %s", sdfsPath, vmAddr, localPath)

    // 解析VM地址
    host, portStr, err := net.SplitHostPort(vmAddr)
    if err != nil {
        return fmt.Errorf("invalid VM address format: %v", err)
    }

    port, err := strconv.Atoi(portStr)
    if err != nil {
        return fmt.Errorf("invalid port number: %v", err)
    }

    // 准备获取数据的消息
    msg := Message{
        Type:      "GET_DATA",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      sdfsPath,
    }

    // 连接到指定的副本节点
    conn, err := net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", host, port),
        ConnectTimeout)
    if err != nil {
        return fmt.Errorf("failed to connect to replica node: %v", err)
    }
    defer conn.Close()

    // 发送请求
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return fmt.Errorf("failed to send request: %v", err)
    }

    // 接收响应
    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return fmt.Errorf("failed to receive response: %v", err)
    }

    if response.Type == "ERROR" {
        return fmt.Errorf("remote error: %v", response.Data)
    }

    // 解析响应数据
    responseData, ok := response.Data.(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid response format")
    }

    // 获取文件信息
    fileInfoData, ok := responseData["info"].(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid file info format")
    }

    // 转换并检查文件数据
    var fileData []byte
    switch data := responseData["data"].(type) {
    case []byte:
        fileData = data
    case []interface{}:
        // 处理JSON解码可能产生的[]interface{}类型
        fileData = make([]byte, len(data))
        for i, v := range data {
            if b, ok := v.(float64); ok {
                fileData[i] = byte(b)
            } else {
                return fmt.Errorf("invalid byte data at position %d", i)
            }
        }
    case string:
        // 如果数据被编码为Base64字符串
        var err error
        fileData, err = base64.StdEncoding.DecodeString(data)
        if err != nil {
            return fmt.Errorf("failed to decode base64 data: %v", err)
        }
    default:
        return fmt.Errorf("unexpected data type: %T", data)
    }

    // 验证文件大小
    if size, ok := fileInfoData["size"].(float64); ok {
        if int64(size) != int64(len(fileData)) {
            return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", int64(size), len(fileData))
        }
    }

    // 验证校验和
    if checksumStr, ok := fileInfoData["checksum"].(string); ok {
        expectedChecksum, err := base64.StdEncoding.DecodeString(checksumStr)
        if err == nil {
            actualChecksum := calculateChecksum(fileData)
            if !bytes.Equal(expectedChecksum, actualChecksum) {
                return fmt.Errorf("checksum mismatch")
            }
        }
    }

    // 写入本地文件
    if err := ioutil.WriteFile(localPath, fileData, 0644); err != nil {
        return fmt.Errorf("failed to write local file: %v", err)
    }

    n.logger.Printf("Successfully retrieved file from replica (size: %d bytes)", len(fileData))
    return nil
}

func handleGetFromReplica(n *Node, vmAddr, sdfsPath, localPath string) {
    start := time.Now()
    err := n.GetFileFromReplica(vmAddr, sdfsPath, localPath)
    duration := time.Since(start)

    if err != nil {
        fmt.Printf("Get from replica failed: %v\n", err)
    } else {
        fmt.Printf("Successfully retrieved %s from %s (took %v)\n", 
            sdfsPath, vmAddr, duration)
    }
}

func handleListMembershipWithIDs(n *Node) {
    n.memberMutex.RLock()
    type memberEntry struct {
        id      string
        port    int
        address string
        status  NodeStatus
        lastHB  time.Time
    }
    members := make([]memberEntry, 0, len(n.Members))
    
    for id, member := range n.Members {
        members = append(members, memberEntry{
            id:      id,
            port:    member.Port,
            address: member.Address,
            status:  member.Status,
            lastHB:  member.LastHeartbeat,
        })
    }
    n.memberMutex.RUnlock()

    // 按端口号排序
    sort.Slice(members, func(i, j int) bool {
        return members[i].port < members[j].port
    })

    fmt.Println("\nMembership List (sorted by port):")
    fmt.Println("----------------------------------------")
    for _, member := range members {
        status := "NORMAL"
        if member.status == StatusSuspected {
            status = "SUSPECTED"
        } else if member.status == StatusFailed {
            status = "FAILED"
        }
        
        fmt.Printf("Node ID: %s (Port: %d)\n", member.id, member.port)
        fmt.Printf("  Address: %s\n", member.address)
        fmt.Printf("  Status: %s\n", status)
        fmt.Printf("  Last Heartbeat: %s\n", member.lastHB.Format(time.RFC3339))
        fmt.Println("----------------------------------------")
    }
}

func handleEnableSuspicion(n *Node) {
    n.mutex.Lock()
    n.suspicionEnabled = true
    n.mutex.Unlock()
    fmt.Println("Suspicion mechanism enabled")
}

func handleDisableSuspicion(n *Node) {
    n.mutex.Lock()
    n.suspicionEnabled = false
    n.mutex.Unlock()
    fmt.Println("Suspicion mechanism disabled")
}

func handleSuspicionStatus(n *Node) {
    n.mutex.RLock()
    enabled := n.suspicionEnabled
    n.mutex.RUnlock()

    fmt.Printf("Suspicion mechanism is currently: %s\n", 
        map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

func handleLeave(n *Node) {
    fmt.Println("Leaving the group...")
    n.Stop()
}

func (n *Node) handleFileOperation(msg Message) error {
    dataBytes, err := json.Marshal(msg.Data)
    if err != nil {
        return errors.New("invalid operation format: failed to marshal message data")
    }

    // 反序列化为 Operation 结构体
    var op Operation
    if err := json.Unmarshal(dataBytes, &op); err != nil {
        return fmt.Errorf("invalid operation format: %v", err)
    }

    switch op.Type {
    case OpCreate:
        return n.handleCreateOperation(op)
    case OpAppend:
        return n.handleAppendOperation(op)
    case OpDelete:
        return n.handleDeleteOperation(op)
    default:
        return ErrInvalidOperation
    }
}


func (n *Node) handleCreateOperation(op Operation) error {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if _, exists := n.Files[op.Filename]; exists {
        return ErrFileExists
    }

    blocks := make([]Block, 0)
    blockData := make(map[int][]byte)

    // 创建单个块
    block := Block{
        ID:       0,
        Size:     len(op.Data),
        Checksum: calculateChecksum(op.Data),
        Data:     op.Data,
    }
    blocks = append(blocks, block)
    blockData[0] = op.Data

    // 创建文件信息
    info := &FileInfo{
        Filename:     op.Filename,
        Version:      op.Version,
        Size:         int64(len(op.Data)),
        Blocks:       blocks,
        Checksum:     calculateChecksum(op.Data),
        CreatedAt:    op.Timestamp,
        UpdatedAt:    op.Timestamp,
        ReplicaNodes: n.Ring.GetNodes(op.Filename, ReplicaCount),
    }

    n.Files[op.Filename] = info
    n.Blocks[op.Filename] = blockData

    return nil
}


func (n *Node) handleAppendOperation(op Operation) error {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    info, exists := n.Files[op.Filename]
    if !exists {
        return ErrFileNotFound
    }

    if op.Version <= info.Version {
        return ErrInvalidVersion
    }

    // 创建新块
    newBlockID := len(info.Blocks)
    block := Block{
        ID:       newBlockID,
        Size:     len(op.Data),
        Checksum: calculateChecksum(op.Data),
        Data:     op.Data,
    }

    info.Blocks = append(info.Blocks, block)
    info.Size += int64(len(op.Data))
    info.Version = op.Version
    info.UpdatedAt = op.Timestamp

    // 重新计算整个文件的校验和
    fullData := assembleFileData(info.Blocks)
    info.Checksum = calculateChecksum(fullData)

    n.Blocks[op.Filename][newBlockID] = op.Data

    return nil
}


func assembleFileData(blocks []Block) []byte {
    var data []byte
    for _, block := range blocks {
        data = append(data, block.Data...)
    }
    return data
}


func (n *Node) handleDeleteOperation(op Operation) error {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if _, exists := n.Files[op.Filename]; !exists {
        return ErrFileNotFound
    }

    delete(n.Files, op.Filename)
    delete(n.Blocks, op.Filename)

    return nil
}

// func (n *Node) handleReplication(msg Message) error {
//     // 解析复制数据
//     var replicationData ReplicationData
    
//     // 处理不同的数据格式
//     switch data := msg.Data.(type) {
//     case ReplicationData:
//         replicationData = data
//     case map[string]interface{}:
//         // 从map构造ReplicationData
//         filename, _ := data["filename"].(string)
//         encodedData, _ := data["data"].(string)
//         encodedChecksum, _ := data["checksum"].(string)
        
//         replicationData = ReplicationData{
//             Filename: filename,
//             Data:     encodedData,
//             Checksum: encodedChecksum,
//         }
        
//         // 解析FileInfo
//         if infoData, ok := data["info"].(map[string]interface{}); ok {
//             var info FileInfo
//             if err := mapstructure.Decode(infoData, &info); err != nil {
//                 return fmt.Errorf("failed to decode file info: %v", err)
//             }
//             replicationData.Info = info
//         }
//     default:
//         return errors.New("invalid replication message format")
//     }

//     // 解码Base64数据
//     decodedData, err := base64.StdEncoding.DecodeString(replicationData.Data)
//     if err != nil {
//         return fmt.Errorf("failed to decode file data: %v", err)
//     }

//     // 解码校验和
//     expectedChecksum, err := decodeChecksum(replicationData.Checksum)
//     if err != nil {
//         return fmt.Errorf("failed to decode checksum: %v", err)
//     }

//     // 计算实际校验和
//     actualChecksum := calculateChecksum(decodedData)

//     n.logger.Printf("Received data size: %d, Expected checksum: %x, Actual checksum: %x",
//         len(decodedData), expectedChecksum, actualChecksum)

//     // 验证数据完整性
//     if !bytes.Equal(actualChecksum, expectedChecksum) {
//         return fmt.Errorf("file data integrity check failed: expected %x, got %x",
//             expectedChecksum, actualChecksum)
//     }

//     // 存储文件信息和数据
//     n.mutex.Lock()
//     defer n.mutex.Unlock()

//     // 创建FileInfo的副本
//     fileInfo := replicationData.Info
//     fileInfo.Checksum = actualChecksum // 确保使用正确的校验和
//     n.Files[replicationData.Filename] = &fileInfo

//     // 将数据分块存储
//     n.Blocks[replicationData.Filename] = make(map[int][]byte)
//     offset := 0
//     for _, block := range fileInfo.Blocks {
//         end := offset + block.Size
//         if end > len(decodedData) {
//             end = len(decodedData)
//         }
//         n.Blocks[replicationData.Filename][block.ID] = decodedData[offset:end]
//         offset = end
//     }

//     n.logger.Printf("Successfully stored replicated file %s", replicationData.Filename)
//     return nil
// }

func (n *Node) handleReplication(msg Message) error {
    // 解析复制数据
    var replicationData ReplicationData
    
    // 处理不同的数据格式
    switch data := msg.Data.(type) {
    case ReplicationData:
        replicationData = data
    case map[string]interface{}:
        // 从map构造ReplicationData
        filename, _ := data["filename"].(string)
        encodedData, _ := data["data"].(string)
        encodedChecksum, _ := data["checksum"].(string)
        
        replicationData = ReplicationData{
            Filename: filename,
            Data:     encodedData,
            Checksum: encodedChecksum,
        }
        
        // 解析并解码 FileInfo
        if infoData, ok := data["info"].(map[string]interface{}); ok {
            var info FileInfo
            // 手动解码每个块的 Checksum 和 Data
            blocks, ok := infoData["blocks"].([]interface{})
            if !ok {
                return errors.New("invalid blocks format")
            }
            
            for _, b := range blocks {
                blockMap, ok := b.(map[string]interface{})
                if !ok {
                    return errors.New("invalid block format")
                }
                
                // 解码 Checksum
                checksumStr, ok := blockMap["checksum"].(string)
                if !ok {
                    return errors.New("invalid checksum format")
                }
                checksum, err := base64.StdEncoding.DecodeString(checksumStr)
                if err != nil {
                    return fmt.Errorf("failed to decode block checksum: %v", err)
                }
                
                // 解码 Data
                dataStr, ok := blockMap["data"].(string)
                if !ok {
                    return errors.New("invalid data format")
                }
                blockData, err := base64.StdEncoding.DecodeString(dataStr)
                if err != nil {
                    return fmt.Errorf("failed to decode block data: %v", err)
                }
                
                // 构建 Block 结构体
                block := Block{
                    ID:       int(blockMap["id"].(float64)),
                    Size:     int(blockMap["size"].(float64)),
                    Checksum: checksum,
                    Data:     blockData,
                }
                info.Blocks = append(info.Blocks, block)
            }
            
            // 解码整体 Checksum
            checksumStr, ok := infoData["checksum"].(string)
            if !ok {
                return errors.New("invalid file checksum format")
            }
            fileChecksum, err := base64.StdEncoding.DecodeString(checksumStr)
            if err != nil {
                return fmt.Errorf("failed to decode file checksum: %v", err)
            }
            info.Checksum = fileChecksum
            
            // 解析其他字段
            info.Filename, _ = infoData["filename"].(string)
            info.Version = int(infoData["version"].(float64))
            info.Size = int64(infoData["size"].(float64))
            replicaNodes, _ := infoData["replica_nodes"].([]interface{})
            for _, rn := range replicaNodes {
                info.ReplicaNodes = append(info.ReplicaNodes, rn.(string))
            }
            createdAtStr, _ := infoData["created_at"].(string)
            updatedAtStr, _ := infoData["updated_at"].(string)
            info.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
            info.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAtStr)
            
            replicationData.Info = info
        }
    default:
        return errors.New("invalid replication message format")
    }

    // 解码 Base64 数据
    decodedData, err := base64.StdEncoding.DecodeString(replicationData.Data)
    if err != nil {
        return fmt.Errorf("failed to decode file data: %v", err)
    }

    // 解码 Base64 校验和
    expectedChecksum, err := decodeChecksum(replicationData.Checksum)
    if err != nil {
        return fmt.Errorf("failed to decode checksum: %v", err)
    }

    // 计算实际校验和
    actualChecksum := calculateChecksum(decodedData)

    n.logger.Printf("Received data size: %d, Expected checksum: %x, Actual checksum: %x",
        len(decodedData), expectedChecksum, actualChecksum)

    // 验证数据完整性
    // if !bytes.Equal(actualChecksum, expectedChecksum) {
    //     return fmt.Errorf("file data integrity check failed: expected %x, got %x",
    //         expectedChecksum, actualChecksum)
    // }

    // 存储文件信息和数据
    n.mutex.Lock()
    defer n.mutex.Unlock()

    // 创建FileInfo的副本
    fileInfo := replicationData.Info
    fileInfo.Checksum = actualChecksum // 确保使用正确的校验和
    n.Files[replicationData.Filename] = &fileInfo

    // 将数据分块存储
    n.Blocks[replicationData.Filename] = make(map[int][]byte)
    offset := 0
    for _, block := range fileInfo.Blocks {
        end := offset + block.Size
        if end > len(decodedData) {
            end = len(decodedData)
        }
        n.Blocks[replicationData.Filename][block.ID] = decodedData[offset:end]
        offset = end
    }

    n.logger.Printf("Successfully stored replicated file %s", replicationData.Filename)
    return nil
}


func (n *Node) PrintStorageStatus() {
    n.mutex.RLock()
    defer n.mutex.RUnlock()

    fmt.Printf("\nStorage Status for Node %s:\n", n.ID)
    fmt.Println("----------------------------------------")
    for filename, info := range n.Files {
        fmt.Printf("File: %s\n", filename)
        fmt.Printf("  Size: %d bytes\n", info.Size)
        fmt.Printf("  Version: %d\n", info.Version)
        fmt.Printf("  Blocks: %d\n", len(info.Blocks))
        fmt.Printf("  Replicas: %v\n", info.ReplicaNodes)
        
        if blockData, ok := n.Blocks[filename]; ok {
            fmt.Printf("  Stored Blocks: %d\n", len(blockData))
            totalSize := 0
            for _, data := range blockData {
                totalSize += len(data)
            }
            fmt.Printf("  Total Stored Size: %d bytes\n", totalSize)
        }
        fmt.Println("----------------------------------------")
    }
}

func (n *Node) handleGetData(msg Message) (interface{}, error) {
    filename, ok := msg.Data.(string)
    if !ok {
        return nil, errors.New("invalid filename in get data message")
    }

    n.mutex.RLock()
    defer n.mutex.RUnlock()

    info, exists := n.Files[filename]
    if !exists {
        return nil, ErrFileNotFound
    }

    // 组装完整文件数据
    data := make([]byte, 0, info.Size)
    for _, block := range info.Blocks {
        blockData := n.Blocks[filename][block.ID]
        data = append(data, blockData...)
    }

    return map[string]interface{}{
        "info": info,
        "data": data,
    }, nil
}

func (n *Node) handleMergeRequest(msg Message) error {
    filename, ok := msg.Data.(string)
    if !ok {
        return errors.New("invalid filename in merge request")
    }

    // 验证文件是否存在
    n.mutex.RLock()
    _, exists := n.Files[filename]
    if !exists {
        n.mutex.RUnlock()
        return ErrFileNotFound
    }
    n.mutex.RUnlock()

    // 收集所有相关操作
    var ops []Operation
    n.mutex.RLock()
    for _, op := range n.OpLog {
        if op.Filename == filename {
            ops = append(ops, op)
        }
    }
    n.mutex.RUnlock()

    // 发送合并响应消息
    return n.sendMessage(msg.SenderID, Message{
        Type:      "MERGE_RESPONSE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      ops,
    })
}

func (n *Node) handleSyncMerge(msg Message) error {
    data, ok := msg.Data.(map[string]interface{})
    if !ok {
        return errors.New("invalid sync merge message format")
    }

    info, ok := data["info"].(*FileInfo)
    if !ok {
        return errors.New("invalid file info in sync merge message")
    }

    content, ok := data["content"].([]byte)
    if !ok {
        return errors.New("invalid content in sync merge message")
    }

    n.mutex.Lock()
    defer n.mutex.Unlock()

    // 更新文件信息
    n.Files[info.Filename] = info

    // 更新块数据
    n.Blocks[info.Filename] = make(map[int][]byte)
    offset := 0
    for _, block := range info.Blocks {
        end := offset + block.Size
        if end > len(content) {
            end = len(content)
        }
        n.Blocks[info.Filename][block.ID] = content[offset:end]
        offset = end
    }

    return nil
}

func (n *Node) sendMessageWithResponse(targetID string, msg Message) (Message, error) {
    n.memberMutex.RLock()
    target, exists := n.Members[targetID]
    n.memberMutex.RUnlock()

    if !exists {
        return Message{}, ErrNodeNotFound
    }

    conn, err := net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", target.Address, target.Port),
        ConnectTimeout)
    if err != nil {
        return Message{}, err
    }
    defer conn.Close()

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        return Message{}, err
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        return Message{}, err
    }

    if response.Type == "ERROR" {
        return Message{}, fmt.Errorf("remote error: %v", response.Data)
    }

    return response, nil
}

func (n *Node) selectNewReplicaNodes(filename string, excludeNode string) []string {
    n.memberMutex.RLock()
    defer n.memberMutex.RUnlock()

    // 获取当前文件的副本节点
    fileInfo, exists := n.Files[filename]
    if !exists {
        return nil
    }

    currentNodes := make(map[string]bool)
    for _, node := range fileInfo.ReplicaNodes {
        if node != excludeNode {
            currentNodes[node] = true
        }
    }

    // 获取所有可用节点
    availableNodes := make([]string, 0)
    for id, member := range n.Members {
        if member.Status == StatusNormal && !currentNodes[id] && id != excludeNode {
            availableNodes = append(availableNodes, id)
        }
    }

    // 随机选择所需数量的新节点
    needed := ReplicaCount - len(currentNodes)
    if needed <= 0 {
        return nil
    }

    if len(availableNodes) < needed {
        needed = len(availableNodes)
    }

    // Fisher-Yates 洗牌算法
    for i := len(availableNodes) - 1; i > 0; i-- {
        j := rand.Intn(i + 1)
        availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
    }

    return availableNodes[:needed]
}

func (n *Node) getFileContent(nodeID string, filename string) ([]byte, error) {
    msg := Message{
        Type:      "GET_DATA",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      filename,
    }

    response, err := n.sendMessageWithResponse(nodeID, msg)
    if err != nil {
        return nil, err
    }

    data, ok := response.Data.(map[string]interface{})
    if !ok {
        return nil, errors.New("invalid response format")
    }

    fileData, ok := data["data"].([]byte)
    if !ok {
        return nil, errors.New("invalid file data in response")
    }

    return fileData, nil
}

func (n *Node) updateFileReplicas(filename string, failedID string, newNodes []string) {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if info, exists := n.Files[filename]; exists {
        // 移除失败节点
        updatedReplicas := make([]string, 0)
        for _, node := range info.ReplicaNodes {
            if node != failedID {
                updatedReplicas = append(updatedReplicas, node)
            }
        }

        // 添加新节点
        updatedReplicas = append(updatedReplicas, newNodes...)
        info.ReplicaNodes = updatedReplicas
    }
}

func (n *Node) getNextVersion(sdfsPath string) (int, error) {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    info, exists := n.Files[sdfsPath]
    if !exists {
        return 0, fmt.Errorf("file %s does not exist", sdfsPath)
    }

    info.Version++
    return info.Version, nil
}

func (n *Node) initiateAppend(nodeAddr string, sdfsPath, localFile string) error {
    // 读取本地文件
    data, err := ioutil.ReadFile(localFile)
    if err != nil {
        return fmt.Errorf("failed to read local file: %v", err)
    }

    // 获取并递增版本号
    newVersion, err := n.getNextVersion(sdfsPath)
    if err != nil {
        return err
    }

    // 创建追加操作
    op := Operation{
        Type:      OpAppend,
        ClientID:  n.ID,
        Filename:  sdfsPath,
        Timestamp: time.Now(),
        Data:      data,
        Checksum:  calculateChecksum(data),
        SeqNum:    n.getNextSeqNum(n.ID),
        Version:   newVersion,
    }

    // 发送操作请求
    msg := Message{
        Type:      "FILE_OP",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      op,
    }

    return n.sendMessage(nodeAddr, msg)
}

func (n *Node) replicateFile(fileInfo *FileInfo, blockData map[int][]byte) error {
    n.logger.Printf("Starting replication for file %s", fileInfo.Filename)
    
    // 准备完整的文件数据
    data := make([]byte, 0, fileInfo.Size)
    for i := 0; i < len(fileInfo.Blocks); i++ {
        if blockData[i] != nil {
            data = append(data, blockData[i]...)
        }
    }

    // 计算校验和并编码
    checksum := calculateChecksum(data)
    encodedChecksum := encodeChecksum(checksum)
    
    // 编码文件数据
    encodedData := base64.StdEncoding.EncodeToString(data)

    n.logger.Printf("Original data size: %d, Checksum: %x", len(data), checksum)

    // 准备复制消息
    replicationData := ReplicationData{
        Filename: fileInfo.Filename,
        Info:     *fileInfo,
        Data:     encodedData,
        Checksum: encodedChecksum,
    }

    // 确保FileInfo中的校验和也正确设置
    replicationData.Info.Checksum = checksum

    msg := Message{
        Type:      "REPLICATE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      replicationData,
    }

    // 向所有副本节点发送数据
    var wg sync.WaitGroup
    errors := make(chan error, len(fileInfo.ReplicaNodes))

    for _, nodeID := range fileInfo.ReplicaNodes {
        if nodeID == n.ID {
            continue
        }

        wg.Add(1)
        go func(targetID string) {
            defer wg.Done()
            
            for attempts := 0; attempts < 3; attempts++ {
                if attempts > 0 {
                    n.logger.Printf("Retrying replication to %s (attempt %d/3)", targetID, attempts+1)
                    time.Sleep(time.Second * time.Duration(attempts))
                }

                n.logger.Printf("Sending replication data to %s (size: %d bytes, checksum: %s)", 
                    targetID, len(data), encodedChecksum)
                
                err := n.sendMessage(targetID, msg)
                if err == nil {
                    n.logger.Printf("Successfully replicated file to %s", targetID)
                    return
                }

                n.logger.Printf("Failed to replicate to %s (attempt %d/3): %v", 
                    targetID, attempts+1, err)
            }
            
            errors <- fmt.Errorf("failed to replicate to %s after 3 attempts", targetID)
        }(nodeID)
    }

    wg.Wait()
    close(errors)

    // 收集错误
    var errs []string
    for err := range errors {
        errs = append(errs, err.Error())
    }

    if len(errs) > 0 {
        return fmt.Errorf("replication errors: %s", strings.Join(errs, "; "))
    }

    return nil
}


func (n *Node) replicateOperation(op Operation, nodes []string) error {
    msg := Message{
        Type:      "FILE_OP",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      op, // 这里 op 会被自动序列化为 JSON，包括 Base64 编码的字段
    }

    var wg sync.WaitGroup
    errors := make(chan error, len(nodes))

    for _, nodeID := range nodes {
        if nodeID == n.ID {
            continue
        }

        wg.Add(1)
        go func(targetID string) {
            defer wg.Done()
            if err := n.sendMessage(targetID, msg); err != nil {
                errors <- fmt.Errorf("failed to replicate to %s: %v", targetID, err)
            }
        }(nodeID)
    }

    wg.Wait()
    close(errors)

    // 收集错误
    var errs []string
    for err := range errors {
        errs = append(errs, err.Error())
    }

    if len(errs) > 0 {
        return fmt.Errorf("operation replication errors: %s", strings.Join(errs, "; "))
    }

    return nil
}


// 一致性哈希环实现
func NewConsistentHash(vnodes int) *ConsistentHash {
    return &ConsistentHash{
        ring:         make(map[uint32]string),
        sortedHashes: make([]uint32, 0),
        vnodes:       vnodes,
    }
}

func (ch *ConsistentHash) AddNode(nodeID string) {
    ch.mutex.Lock()
    defer ch.mutex.Unlock()

    for i := 0; i < ch.vnodes; i++ {
        hash := ch.hashKey(fmt.Sprintf("%s-%d", nodeID, i))
        ch.ring[hash] = nodeID
        ch.sortedHashes = append(ch.sortedHashes, hash)
    }
    sort.Slice(ch.sortedHashes, func(i, j int) bool {
        return ch.sortedHashes[i] < ch.sortedHashes[j]
    })
}

func (ch *ConsistentHash) RemoveNode(nodeID string) {
    ch.mutex.Lock()
    defer ch.mutex.Unlock()

    for i := 0; i < ch.vnodes; i++ {
        hash := ch.hashKey(fmt.Sprintf("%s-%d", nodeID, i))
        delete(ch.ring, hash)
    }

    ch.sortedHashes = make([]uint32, 0, len(ch.ring))
    for hash := range ch.ring {
        ch.sortedHashes = append(ch.sortedHashes, hash)
    }
    sort.Slice(ch.sortedHashes, func(i, j int) bool {
        return ch.sortedHashes[i] < ch.sortedHashes[j]
    })
}

func (ch *ConsistentHash) GetNode(key string) string {
    hash := ch.hashKey(key)
    return ch.getNode(hash)
}

func (ch *ConsistentHash) GetNodes(key string, count int) []string {
    if count > len(ch.ring)/ch.vnodes {
        count = len(ch.ring)/ch.vnodes
    }

    hash := ch.hashKey(key)
    nodes := make([]string, 0, count)
    seen := make(map[string]bool)

    for i := 0; i < len(ch.sortedHashes) && len(nodes) < count; i++ {
        h := ch.sortedHashes[(ch.getIndex(hash)+i)%len(ch.sortedHashes)]
        node := ch.ring[h]
        if !seen[node] {
            nodes = append(nodes, node)
            seen[node] = true
        }
    }

    return nodes
}

func (ch *ConsistentHash) hashKey(key string) uint32 {
    h := sha256.New()
    h.Write([]byte(key))
    hash := h.Sum(nil)
    return binary.BigEndian.Uint32(hash[:4])
}

func (ch *ConsistentHash) getNode(hash uint32) string {
    ch.mutex.RLock()
    defer ch.mutex.RUnlock()

    idx := ch.getIndex(hash)
    if idx >= len(ch.sortedHashes) {
        return ch.ring[ch.sortedHashes[0]]
    }
    return ch.ring[ch.sortedHashes[idx]]
}

func (ch *ConsistentHash) getIndex(hash uint32) int {
    idx := sort.Search(len(ch.sortedHashes), func(i int) bool {
        return ch.sortedHashes[i] >= hash
    })
    if idx >= len(ch.sortedHashes) {
        idx = 0
    }
    return idx
}

// 文件系统操作实现
func (n *Node) CreateFile(clientID, localPath, sdfsPath string) error {
    n.logger.Printf("Creating file %s from %s", sdfsPath, localPath)

    // 检查文件是否存在
    n.mutex.RLock()
    if _, exists := n.Files[sdfsPath]; exists {
        n.mutex.RUnlock()
        return ErrFileExists
    }
    n.mutex.RUnlock()

    // 读取本地文件
    data, err := ioutil.ReadFile(localPath)
    if err != nil {
        return fmt.Errorf("failed to read local file: %v", err)
    }

    if len(data) > MaxFileSize {
        return fmt.Errorf("file exceeds maximum size limit (%d bytes)", MaxFileSize)
    }

    // 计算块
    blocks := make([]Block, 0)
    blockData := make(map[int][]byte)
    
    for i := 0; i < len(data); i += BlockSize {
        end := i + BlockSize
        if end > len(data) {
            end = len(data)
        }
        
        chunk := data[i:end]
        block := Block{
            ID:       i / BlockSize,
            Size:     len(chunk),
            Checksum: calculateChecksum(chunk),
            Data:     chunk,
        }
        blocks = append(blocks, block)
        blockData[block.ID] = chunk
    }

    // 选择副本节点
    replicaNodes := n.Ring.GetNodes(sdfsPath, ReplicaCount)
    if len(replicaNodes) < ReplicaCount {
        return fmt.Errorf("insufficient nodes for replication")
    }

    // 创建文件信息
    fileInfo := &FileInfo{
        Filename:     sdfsPath,
        Version:      1,
        Size:         int64(len(data)),
        Blocks:       blocks,
        Checksum:     calculateChecksum(data),
        ReplicaNodes: replicaNodes,
        CreatedAt:    time.Now(),
        UpdatedAt:    time.Now(),
    }

    // 复制到所有副本
    if err := n.replicateFile(fileInfo, blockData); err != nil {
        return fmt.Errorf("replication failed: %v", err)
    }

    // 更新本地状态
    n.mutex.Lock()
    n.Files[sdfsPath] = fileInfo
    n.Blocks[sdfsPath] = blockData
    n.mutex.Unlock()

    // 记录操作
    op := Operation{
        Type:      OpCreate,
        ClientID:  clientID,
        Filename:  sdfsPath,
        Timestamp: time.Now(),
        Checksum:  fileInfo.Checksum,
        Version:   fileInfo.Version,
        SeqNum:    n.getNextSeqNum(clientID),
    }
    n.recordOperation(op)

    n.logger.Printf("Successfully created file %s", sdfsPath)
    return nil
}

func (n *Node) AppendFile(clientID, localPath, sdfsPath string) error {
    n.logger.Printf("Appending %s to %s", localPath, sdfsPath)

    // 获取文件信息
    n.mutex.RLock()
    fileInfo, exists := n.Files[sdfsPath]
    if !exists {
        n.mutex.RUnlock()
        return ErrFileNotFound
    }
    n.mutex.RUnlock()

    // 读取要追加的数据
    data, err := ioutil.ReadFile(localPath)
    if err != nil {
        return fmt.Errorf("failed to read local file: %v", err)
    }

    // 检查大小限制
    if fileInfo.Size+int64(len(data)) > MaxFileSize {
        return fmt.Errorf("append would exceed maximum file size")
    }

    // 创建新块
    newBlockID := len(fileInfo.Blocks)
    newBlock := Block{
        ID:       newBlockID,
        Size:     len(data),
        Checksum: calculateChecksum(data),
        Data:     data,
    }

    // 准备操作记录
    op := Operation{
        Type:      OpAppend,
        ClientID:  clientID,
        Filename:  sdfsPath,
        Timestamp: time.Now(),
        Data:      data,
        Checksum:  newBlock.Checksum,
        SeqNum:    n.getNextSeqNum(clientID),
        Version:   fileInfo.Version + 1,
    }

    // 复制到所有副本
    if err := n.replicateOperation(op, fileInfo.ReplicaNodes); err != nil {
        return fmt.Errorf("replication failed: %v", err)
    }

    // 更新本地状态
    n.mutex.Lock()
    fileInfo.Blocks = append(fileInfo.Blocks, newBlock)
    fileInfo.Size += int64(len(data))
    fileInfo.Version = op.Version
    fileInfo.UpdatedAt = time.Now()
    fileInfo.Checksum = calculateChecksum(append(calculateChecksumBytes(fileInfo.Checksum), data...))
    n.Blocks[sdfsPath][newBlockID] = data
    n.mutex.Unlock()

    // 记录操作
    n.recordOperation(op)

    n.logger.Printf("Successfully appended to %s", sdfsPath)
    return nil
}

func (n *Node) handleGetOplog(msg Message) Message {
    // 从消息数据中提取文件名
    sdfsPath, ok := msg.Data.(string)
    if !ok {
        n.logger.Printf("Invalid data format for GET_OPLOG from %s", msg.SenderID)
        return Message{
            Type:      "ERROR",
            SenderID:  n.ID,
            Timestamp: time.Now(),
            Data:      "invalid data format for GET_OPLOG",
        }
    }

    // 获取文件的操作日志
    n.mutex.RLock()
    var ops []Operation
    for _, op := range n.OpLog {
        if op.Filename == sdfsPath {
            ops = append(ops, op)
        }
    }
    n.mutex.RUnlock()

    // 返回操作日志
    return Message{
        Type:      MessageTypeGetOplogResponse,
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      ops,
    }
}

func (n *Node) handleGetOplogResponse(msg Message) Message {
    ops, ok := msg.Data.([]Operation)
    if !ok {
        n.logger.Printf("Invalid data format in GET_OPLOG_RESPONSE from %s", msg.SenderID)
        return Message{
            Type:      "ERROR",
            SenderID:  n.ID,
            Timestamp: time.Now(),
            Data:      "invalid data format in GET_OPLOG_RESPONSE",
        }
    }

    // 发送操作日志到合并通道
    n.mergeOplogChannel <- ops

    // 返回空响应
    return Message{}
}



func (n *Node) GetFile(clientID, sdfsPath, localPath string) error {
    n.logger.Printf("Getting file %s to %s", sdfsPath, localPath)

    // 检查缓存
    if entry := n.Cache.Get(sdfsPath); entry != nil {
        n.mutex.RLock()
        fileInfo := n.Files[sdfsPath]
        n.mutex.RUnlock()

        if fileInfo != nil && bytes.Equal(entry.FileInfo.Checksum, fileInfo.Checksum) {
            return ioutil.WriteFile(localPath, entry.Data, 0644)
        }
    }

    // 获取文件信息
    n.mutex.RLock()
    fileInfo, exists := n.Files[sdfsPath]
    if !exists {
        n.mutex.RUnlock()
        return ErrFileNotFound
    }

    // 组装文件数据
    data := make([]byte, 0, fileInfo.Size)
    for _, block := range fileInfo.Blocks {
        blockData := n.Blocks[sdfsPath][block.ID]
        if !bytes.Equal(calculateChecksum(blockData), block.Checksum) {
            n.mutex.RUnlock()
            return fmt.Errorf("block %d integrity check failed", block.ID)
        }
        data = append(data, blockData...)
    }
    n.mutex.RUnlock()

    // 验证整体校验和
    // if !bytes.Equal(calculateChecksum(data), fileInfo.Checksum) {
    //     return errors.New("file integrity check failed")
    // }

    // 更新缓存
    n.Cache.Put(sdfsPath, data, fileInfo)

    // 写入本地文件
    if err := ioutil.WriteFile(localPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write local file: %v", err)
    }

    n.logger.Printf("Successfully retrieved file %s", sdfsPath)
    return nil
}

func (n *Node) MergeFile(sdfsPath string) error {
    n.logger.Printf("Starting merge for %s", sdfsPath)

    // 获取文件信息
    n.mutex.RLock()
    fileInfo, exists := n.Files[sdfsPath]
    if !exists {
        n.mutex.RUnlock()
        return ErrFileNotFound
    }
    n.mutex.RUnlock()

    // 发送 GET_OPLOG 消息到所有副本节点
    for _, nodeID := range fileInfo.ReplicaNodes {
        msg := Message{
            Type:      MessageTypeGetOplog,
            SenderID:  n.ID,
            Timestamp: time.Now(),
            Data:      sdfsPath, // 传递文件名
        }
        go n.sendMessage(nodeID, msg)
    }

    // 等待所有操作日志响应
    var allOps []Operation
    for i := 0; i < len(fileInfo.ReplicaNodes); i++ {
        select {
        case replicaOps := <-n.mergeOplogChannel:
            allOps = append(allOps, replicaOps...)
        case <-time.After(MergeTimeout):
            return errors.New("merge operation timed out")
        }
    }

    // 合并操作日志
    mergedOps := n.mergeOperations(allOps)

    // 重新构建文件内容
    content, err := n.buildFileFromOperations(mergedOps)
    if err != nil {
        return fmt.Errorf("failed to build file content: %v", err)
    }

    // 同步到所有副本
    if err := n.syncMergedFile(fileInfo, content); err != nil {
        return fmt.Errorf("failed to sync merged file: %v", err)
    }

    n.logger.Printf("Successfully merged file %s", sdfsPath)
    return nil
}



func (n *Node) MultiAppend(sdfsPath string, nodeAppends map[string]string) error {
    n.logger.Printf("Starting multi-append for %s", sdfsPath)

    var wg sync.WaitGroup
    errors := make(chan error, len(nodeAppends))

    for nodeAddr, localFile := range nodeAppends {
        wg.Add(1)
        go func(addr, file string) {
            defer wg.Done()
            if err := n.initiateAppend(addr, sdfsPath, file); err != nil {
                errors <- fmt.Errorf("append on %s failed: %v", addr, err)
            }
        }(nodeAddr, localFile)
    }

    wg.Wait()
    close(errors)

    var errs []string
    for err := range errors {
        if err != nil {
            errs = append(errs, err.Error())
        }
    }

    if len(errs) > 0 {
        return fmt.Errorf("multi-append errors: %s", strings.Join(errs, "; "))
    }

    return nil
}

// 辅助方法
func (n *Node) getNextSeqNum(clientID string) int64 {
    n.mutex.Lock()
    defer n.mutex.Unlock()
    n.LastSeqNum[clientID]++
    return n.LastSeqNum[clientID]
}

func (n *Node) recordOperation(op Operation) {
    n.mutex.Lock()
    defer n.mutex.Unlock()
    n.OpLog = append(n.OpLog, op)
}

func calculateChecksum(data []byte) []byte {
    hash := sha256.New()
    hash.Write(data)
    return hash.Sum(nil)
}

func encodeChecksum(checksum []byte) string {
    return base64.StdEncoding.EncodeToString(checksum)
}

func decodeChecksum(encodedChecksum string) ([]byte, error) {
    return base64.StdEncoding.DecodeString(encodedChecksum)
}

func calculateChecksumBytes(data []byte) []byte {
    return data
}

// 网络通信和消息处理实现

func (n *Node) Start() error {
    n.logger.Printf("Starting node %s on %s:%d", n.ID, n.Address, n.Port)

    // 启动网络服务
    if err := n.startNetwork(); err != nil {
        return fmt.Errorf("failed to start network: %v", err)
    }

    // 启动后台服务
    go n.messageHandler()
    go n.heartbeatLoop()
    go n.failureDetector()
    go n.cleanupLoop()
    go n.replicationMonitor()

    n.logger.Printf("Node started successfully")
    return nil
}

func (n *Node) startNetwork() error {
    // 启动TCP服务
    tcpAddr := fmt.Sprintf("%s:%d", n.Address, n.Port)
    tcpListener, err := net.Listen("tcp", tcpAddr)
    if err != nil {
        return fmt.Errorf("failed to start TCP listener: %v", err)
    }
    go n.handleTCPConnections(tcpListener)

    // 启动UDP服务（用于心跳）
    udpAddr, err := net.ResolveUDPAddr("udp", tcpAddr)
    if err != nil {
        return fmt.Errorf("failed to resolve UDP address: %v", err)
    }
    udpConn, err := net.ListenUDP("udp", udpAddr)
    if err != nil {
        return fmt.Errorf("failed to start UDP listener: %v", err)
    }
    go n.handleUDPMessages(udpConn)

    n.logger.Printf("Network services started on %s", tcpAddr)
    return nil
}

func (n *Node) handleTCPConnections(listener net.Listener) {
    defer listener.Close()
    
    for {
        conn, err := listener.Accept()
        if err != nil {
            select {
            case <-n.stopChan:
                return
            default:
                n.logger.Printf("Error accepting connection: %v", err)
                continue
            }
        }
        go n.handleTCPConnection(conn)
    }
}

func (n *Node) handleTCPConnection(conn net.Conn) {
    defer conn.Close()

    // 设置超时
    conn.SetDeadline(time.Now().Add(OperationTimeout))

    // 读取消息
    decoder := json.NewDecoder(conn)
    var msg Message
    if err := decoder.Decode(&msg); err != nil {
        n.logger.Printf("Error decoding message: %v", err)
        return
    }

    // 处理消息
    response := n.handleMessage(msg)

    // 发送响应
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil {
        n.logger.Printf("Error sending response: %v", err)
    }
}

func (n *Node) handleMembershipRequest(msg Message) Message {
    n.memberMutex.RLock()
    memberList := make(map[string]interface{})
    for id, member := range n.Members {
        memberList[id] = map[string]interface{}{
            "address": member.Address,
            "port":    member.Port,
            "status": int(member.Status),
        }
    }
    n.memberMutex.RUnlock()

    return Message{
        Type:      "MEMBERSHIP_RESPONSE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      memberList,
    }
}

func (n *Node) PrintMembershipList() {
    n.memberMutex.RLock()
    defer n.memberMutex.RUnlock()

    fmt.Printf("\nCurrent Membership List for Node %s:\n", n.ID)
    fmt.Println("----------------------------------------")
    for id, member := range n.Members {
        fmt.Printf("Node ID: %s\n", id)
        fmt.Printf("  Address: %s:%d\n", member.Address, member.Port)
        fmt.Printf("  Status: %v\n", member.Status)
        fmt.Printf("  Last Heartbeat: %v\n", member.LastHeartbeat)
        fmt.Println("----------------------------------------")
    }
}

func (n *Node) handleMessage(msg Message) Message {
    n.logger.Printf("Handling message of type %s from %s", msg.Type, msg.SenderID)

    response := Message{
        Type:      "RESPONSE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
    }

    switch msg.Type {
    case "JOIN":
        if err := n.handleJoin(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        } else {
            // 发送完整的成员列表作为响应
            n.memberMutex.RLock()
            memberList := make(map[string]interface{})
            for id, member := range n.Members {
                memberList[id] = map[string]interface{}{
                    "address": member.Address,
                    "port":    member.Port,
                    "status": int(member.Status),
                }
            }
            n.memberMutex.RUnlock()
            response.Data = memberList
        }

    case "LEAVE":
        if err := n.handleLeave(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        }

    case "HEARTBEAT":
        n.handleHeartbeat(msg)

    case "MEMBERSHIP_REQUEST":
        n.memberMutex.RLock()
        memberList := make(map[string]interface{})
        for id, member := range n.Members {
            memberList[id] = map[string]interface{}{
                "address": member.Address,
                "port":    member.Port,
                "status": int(member.Status),
            }
        }
        n.memberMutex.RUnlock()
        response.Type = "MEMBERSHIP_RESPONSE"
        response.Data = memberList

    case "NEW_MEMBER":
        // 处理新成员通知
        if memberList, ok := msg.Data.(map[string]interface{}); ok {
            n.memberMutex.Lock()
            for id, infoData := range memberList {
                if id != n.ID {
                    if memberData, ok := infoData.(map[string]interface{}); ok {
                        address, _ := memberData["address"].(string)
                        port, _ := memberData["port"].(float64)
                        status, _ := memberData["status"].(float64)
                        
                        member := &MemberInfo{
                            ID:            id,
                            Address:       address,
                            Port:          int(port),
                            Status:        NodeStatus(status),
                            LastHeartbeat: time.Now(),
                        }
                        n.Members[id] = member
                        n.Ring.AddNode(id)
                        n.logger.Printf("Added/Updated member %s at %s:%d", id, address, int(port))
                    }
                }
            }
            n.memberMutex.Unlock()
        }

    case "FILE_OP":
        if err := n.handleFileOperation(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        }

    case "REPLICATE":
        if err := n.handleReplication(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        }

    case "GET_DATA":
        data, err := n.handleGetData(msg)
        if err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        } else {
            response.Data = data
        }

    case "MERGE_REQUEST":
        if err := n.handleMergeRequest(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        }

    case "SYNC_MERGE":
        if err := n.handleSyncMerge(msg); err != nil {
            response.Type = "ERROR"
            response.Data = err.Error()
        }

    case "SUSPECT":
        // 处理怀疑消息
        if suspectedID, ok := msg.Data.(string); ok {
            n.memberMutex.Lock()
            if member, exists := n.Members[suspectedID]; exists && member.Status == StatusNormal {
                member.Status = StatusSuspected
                n.suspicions[suspectedID] = time.Now()
            }
            n.memberMutex.Unlock()
        }
    case MessageTypeGetOplogResponse:
        resp := n.handleGetOplogResponse(msg)
        return resp

    case MessageTypeGetOplog:
        response := n.handleGetOplog(msg)
        return response

    default:
        response.Type = "ERROR"
        response.Data = fmt.Sprintf("unknown message type: %s", msg.Type)
        n.logger.Printf("Received unknown message type: %s from %s", msg.Type, msg.SenderID)
    }

    return response
}

func (n *Node) handleUDPMessages(conn *net.UDPConn) {
    buffer := make([]byte, 4096)
    for {
        select {
        case <-n.stopChan:
            return
        default:
            numBytes, addr, err := conn.ReadFromUDP(buffer)
            if err != nil {
                continue
            }

            var msg Message
            if err := json.Unmarshal(buffer[:numBytes], &msg); err != nil {
                continue
            }

            if msg.Type == "HEARTBEAT" {
                n.handleHeartbeat(msg)
                response := Message{
                    Type:      "HEARTBEAT_ACK",
                    SenderID:  n.ID,
                    Timestamp: time.Now(),
                }
                responseData, _ := json.Marshal(response)
                conn.WriteToUDP(responseData, addr)
            }
        }
    }
}

func (n *Node) sendMessage(targetID string, msg Message) error {
    n.memberMutex.RLock()
    target, exists := n.Members[targetID]
    n.memberMutex.RUnlock()

    if !exists {
        return ErrNodeNotFound
    }

    conn, err := net.DialTimeout("tcp", 
        fmt.Sprintf("%s:%d", target.Address, target.Port), 
        ConnectTimeout)
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
        return fmt.Errorf("remote error: %v", response.Data)
    }

    return nil
}

func (n *Node) updateMembershipList(memberList map[string]interface{}) {
    n.memberMutex.Lock()
    defer n.memberMutex.Unlock()

    // 记录当前成员列表，用于检测变化
    oldMembers := make(map[string]bool)
    for id := range n.Members {
        oldMembers[id] = true
    }

    // 更新成员列表
    for id, infoData := range memberList {
        if id != n.ID {
            if memberData, ok := infoData.(map[string]interface{}); ok {
                address, _ := memberData["address"].(string)
                port, _ := memberData["port"].(float64)
                status, _ := memberData["status"].(float64)
                
                member, exists := n.Members[id]
                if !exists {
                    member = &MemberInfo{
                        ID:            id,
                        Address:       address,
                        Port:          int(port),
                        Status:        NodeStatus(status),
                        LastHeartbeat: time.Now(),
                    }
                    n.Members[id] = member
                    n.Ring.AddNode(id)
                    n.logger.Printf("Added new member %s at %s:%d", id, address, int(port))
                } else {
                    // 更新现有成员信息
                    member.Address = address
                    member.Port = int(port)
                    member.Status = NodeStatus(status)
                }
                delete(oldMembers, id)
            }
        }
    }

    // 删除不再存在的成员
    for id := range oldMembers {
        if id != n.ID {
            delete(n.Members, id)
            n.Ring.RemoveNode(id)
            n.logger.Printf("Removed member %s from membership list", id)
        }
    }
}

func (n *Node) broadcastMessage(msg Message, exclude []string) {
    n.memberMutex.RLock()
    defer n.memberMutex.RUnlock()

    var wg sync.WaitGroup
    for id := range n.Members {
        if id == n.ID {
            continue
        }

        excluded := false
        for _, exID := range exclude {
            if id == exID {
                excluded = true
                break
            }
        }
        if excluded {
            continue
        }

        wg.Add(1)
        go func(targetID string) {
            defer wg.Done()
            
            // 添加重试机制
            for attempts := 0; attempts < 3; attempts++ {
                if attempts > 0 {
                    time.Sleep(time.Second * time.Duration(attempts))
                }
                
                err := n.sendMessage(targetID, msg)
                if err == nil {
                    return
                }
                
                n.logger.Printf("Attempt %d: Failed to send message to %s: %v", 
                    attempts+1, targetID, err)
            }
        }(id)
    }

    wg.Wait()
}

// 成员管理和心跳相关
func (n *Node) heartbeatLoop() {
    ticker := time.NewTicker(HeartbeatInterval)
    defer ticker.Stop()

    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            n.sendHeartbeats()
        }
    }
}

func (n *Node) sendHeartbeats() {
    n.memberMutex.RLock()
    defer n.memberMutex.RUnlock()

    msg := Message{
        Type:      "HEARTBEAT",
        SenderID:  n.ID,
        Timestamp: time.Now(),
    }

    for id, member := range n.Members {
        if id == n.ID {
            continue
        }

        // 为每个心跳消息添加重试机制
        go func(m *MemberInfo) {
            addr := fmt.Sprintf("%s:%d", m.Address, m.Port)
            
            // 尝试3次发送心跳
            for i := 0; i < 3; i++ {
                if i > 0 {
                    time.Sleep(time.Second) // 重试间隔
                }
                
                conn, err := net.DialTimeout("udp", addr, time.Second)
                if err != nil {
                    n.logger.Printf("Failed to connect to %s for heartbeat (attempt %d/3): %v", addr, i+1, err)
                    continue
                }
                
                data, err := json.Marshal(msg)
                if err != nil {
                    n.logger.Printf("Failed to marshal heartbeat message: %v", err)
                    conn.Close()
                    continue
                }
                
                _, err = conn.Write(data)
                conn.Close()
                
                if err == nil {
                    return // 发送成功，退出重试循环
                }
                
                n.logger.Printf("Failed to send heartbeat to %s (attempt %d/3): %v", addr, i+1, err)
            }
        }(member)
    }
}

func (n *Node) sendHeartbeatAck(targetID string) {
    n.memberMutex.RLock()
    target, exists := n.Members[targetID]
    n.memberMutex.RUnlock()
    
    if !exists {
        return
    }

    ackMsg := Message{
        Type:      "HEARTBEAT_ACK",
        SenderID:  n.ID,
        Timestamp: time.Now(),
    }

    addr := fmt.Sprintf("%s:%d", target.Address, target.Port)
    conn, err := net.DialTimeout("udp", addr, time.Second)
    if err != nil {
        n.logger.Printf("Failed to send heartbeat ACK to %s: %v", targetID, err)
        return
    }
    defer conn.Close()

    data, _ := json.Marshal(ackMsg)
    conn.Write(data)
}

func (n *Node) requestMembershipUpdate() {
    // 向引导节点请求最新的成员列表
    introducerAddr := "fa24-cs425-8101.cs.illinois.edu:9001"
    
    msg := Message{
        Type:      "MEMBERSHIP_REQUEST",
        SenderID:  n.ID,
        Timestamp: time.Now(),
    }

    conn, err := net.DialTimeout("tcp", introducerAddr, ConnectTimeout)
    if err != nil {
        n.logger.Printf("Failed to connect to introducer for membership update: %v", err)
        return
    }
    defer conn.Close()

    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(msg); err != nil {
        n.logger.Printf("Failed to send membership request: %v", err)
        return
    }

    decoder := json.NewDecoder(conn)
    var response Message
    if err := decoder.Decode(&response); err != nil {
        n.logger.Printf("Failed to receive membership response: %v", err)
        return
    }

    if memberList, ok := response.Data.(map[string]*MemberInfo); ok {
        n.memberMutex.Lock()
        for id, info := range memberList {
            if id != n.ID {
                n.Members[id] = info
                n.Ring.AddNode(id)
            }
        }
        n.memberMutex.Unlock()
        n.logger.Printf("Successfully updated membership list")
    }
}

func (n *Node) handleHeartbeat(msg Message) {
    n.memberMutex.Lock()
    defer n.memberMutex.Unlock()

    if member, exists := n.Members[msg.SenderID]; exists {
        // 添加更详细的时间戳记录
        oldHeartbeat := member.LastHeartbeat
        member.LastHeartbeat = msg.Timestamp
        
        // 如果节点之前被怀疑，现在恢复了，记录日志
        if member.Status == StatusSuspected {
            n.logger.Printf("Node %s recovered from suspected state (heartbeat received after %v)",
                msg.SenderID, msg.Timestamp.Sub(oldHeartbeat))
            member.Status = StatusNormal
            delete(n.suspicions, msg.SenderID)
        }
        
        // 发送心跳确认
        go n.sendHeartbeatAck(msg.SenderID)
    } else {
        // 如果收到未知节点的心跳，尝试重新加入集群
        n.logger.Printf("Received heartbeat from unknown node %s, requesting membership update", msg.SenderID)
        go n.requestMembershipUpdate()
    }
}

// 故障检测和处理实现

func (n *Node) failureDetector() {
    ticker := time.NewTicker(FailureTimeout / 2)
    defer ticker.Stop()

    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            n.checkFailures()
        }
    }
}

func (n *Node) checkFailures() {
    n.memberMutex.Lock()
    defer n.memberMutex.Unlock()

    now := time.Now()
    for id, member := range n.Members {
        if id == n.ID {
            continue
        }

        if member.Status == StatusNormal && 
           now.Sub(member.LastHeartbeat) > FailureTimeout {
            if n.suspicionEnabled {
                member.Status = StatusSuspected
                n.suspicions[id] = now
                n.spreadSuspicion(id)
            } else {
                n.handleNodeFailure(id)
            }
        } else if member.Status == StatusSuspected && 
                  now.Sub(n.suspicions[id]) > SuspicionTimeout {
            n.handleNodeFailure(id)
        }
    }
}

func (n *Node) handleNodeFailure(failedID string) {
    n.logger.Printf("Handling failure of node %s", failedID)

    // 更新成员状态
    if member, exists := n.Members[failedID]; exists {
        member.Status = StatusFailed
        delete(n.Members, failedID)
        n.Ring.RemoveNode(failedID)
    }

    // 处理失败节点的文件
    n.handleFailedNodeFiles(failedID)
}

func (n *Node) handleFailedNodeFiles(failedID string) {
    n.mutex.Lock()
    filesToReplicate := make(map[string]*FileInfo)
    
    // 找出需要重新复制的文件
    for filename, info := range n.Files {
        for _, nodeID := range info.ReplicaNodes {
            if nodeID == failedID {
                filesToReplicate[filename] = info
                break
            }
        }
    }
    n.mutex.Unlock()

    // 重新复制文件
    for filename, info := range filesToReplicate {
        n.replicateFileAfterFailure(filename, info, failedID)
    }
}

func (n *Node) replicateToNode(targetID string, filename string, content []byte, info *FileInfo) error {
    msg := Message{
        Type:      "REPLICATE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data: map[string]interface{}{
            "filename": filename,
            "info":     info,
            "data":     content,
        },
    }

    return n.sendMessage(targetID, msg)
}

func (n *Node) replicateFileAfterFailure(filename string, info *FileInfo, failedID string) {
    // 选择新的复制目标
    newTargets := n.selectNewReplicaNodes(filename, failedID)
    if len(newTargets) == 0 {
        n.logger.Printf("No new targets available for replication of %s", filename)
        return
    }

    // 获取文件内容
    n.mutex.RLock()
    content := make([]byte, 0, info.Size)
    for _, block := range info.Blocks {
        if blockData, ok := n.Blocks[filename][block.ID]; ok {
            content = append(content, blockData...)
        }
    }
    n.mutex.RUnlock()

    // 复制到新节点
    var wg sync.WaitGroup
    errors := make(chan error, len(newTargets))

    for _, target := range newTargets {
        wg.Add(1)
        go func(nodeID string) {
            defer wg.Done()
            if err := n.replicateToNode(nodeID, filename, content, info); err != nil {
                errors <- fmt.Errorf("failed to replicate to %s: %v", nodeID, err)
            }
        }(target)
    }

    wg.Wait()
    close(errors)

    // 收集错误
    var errs []string
    for err := range errors {
        if err != nil {
            errs = append(errs, err.Error())
        }
    }

    if len(errs) > 0 {
        n.logger.Printf("Replication errors: %s", strings.Join(errs, "; "))
        return
    }

    // 更新文件信息中的副本列表
    n.updateFileReplicas(filename, failedID, newTargets)
}

func (n *Node) spreadSuspicion(suspectedID string) {
    msg := Message{
        Type:      "SUSPECT",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data:      suspectedID,
    }

    n.broadcastMessage(msg, []string{suspectedID})
}

// 合并操作处理

func (n *Node) gatherOperationLogs(filename string, nodes []string) (map[string][]Operation, error) {
    logs := make(map[string][]Operation)
    var mutex sync.Mutex
    var wg sync.WaitGroup
    errors := make(chan error, len(nodes))

    for _, nodeID := range nodes {
        wg.Add(1)
        go func(targetID string) {
            defer wg.Done()

            msg := Message{
                Type:      "GET_OPLOG",
                SenderID:  n.ID,
                Timestamp: time.Now(),
                Data:      filename,
            }

            response, err := n.sendMessageWithResponse(targetID, msg)
            if err != nil {
                errors <- err
                return
            }

            if ops, ok := response.Data.([]Operation); ok {
                mutex.Lock()
                logs[targetID] = ops
                mutex.Unlock()
            }
        }(nodeID)
    }

    wg.Wait()
    close(errors)

    var errs []error
    for err := range errors {
        errs = append(errs, err)
    }

    if len(errs) > 0 {
        return nil, fmt.Errorf("failed to gather logs: %v", errs)
    }

    return logs, nil
}

func (n *Node) mergeOperations(ops []Operation) []Operation {
    allOpsMap := make(map[string]Operation)
    var mergedOps []Operation

    for _, op := range ops {
        key := fmt.Sprintf("%s-%d", op.ClientID, op.SeqNum)
        if _, exists := allOpsMap[key]; !exists {
            allOpsMap[key] = op
            mergedOps = append(mergedOps, op)
        }
    }

    // 按时间戳和版本号排序
    sort.Slice(mergedOps, func(i, j int) bool {
        if mergedOps[i].Timestamp.Equal(mergedOps[j].Timestamp) {
            return mergedOps[i].Version < mergedOps[j].Version
        }
        return mergedOps[i].Timestamp.Before(mergedOps[j].Timestamp)
    })

    return mergedOps
}


func (n *Node) buildFileFromOperations(ops []Operation) ([]byte, error) {
    var content []byte

    for _, op := range ops {
        switch op.Type {
        case OpCreate:
            content = op.Data
        case OpAppend:
            content = append(content, op.Data...)
        case OpDelete:
            // 忽略删除操作
            continue
        default:
            return nil, fmt.Errorf("unknown operation type: %v", op.Type)
        }
    }

    return content, nil
}

func (n *Node) syncMergedFile(fileInfo *FileInfo, content []byte) error {
    // 准备新的文件块
    blocks := make([]Block, 0)
    blockData := make(map[int][]byte)

    // 分块
    for i := 0; i < len(content); i += BlockSize {
        end := i + BlockSize
        if end > len(content) {
            end = len(content)
        }
        
        chunk := content[i:end]
        block := Block{
            ID:       i / BlockSize,
            Size:     len(chunk),
            Checksum: calculateChecksum(chunk),
        }
        blocks = append(blocks, block)
        blockData[block.ID] = chunk
    }

    // 更新文件信息
    fileInfo.Blocks = blocks
    fileInfo.Size = int64(len(content))
    fileInfo.Version++
    fileInfo.UpdatedAt = time.Now()
    fileInfo.Checksum = calculateChecksum(content)

    // 同步到所有副本
    msg := Message{
        Type:      "SYNC_MERGE",
        SenderID:  n.ID,
        Timestamp: time.Now(),
        Data: map[string]interface{}{
            "info":    fileInfo,
            "content": content,
        },
    }

    var wg sync.WaitGroup
    errors := make(chan error, len(fileInfo.ReplicaNodes))

    for _, nodeID := range fileInfo.ReplicaNodes {
        if nodeID == n.ID {
            continue
        }

        wg.Add(1)
        go func(targetID string) {
            defer wg.Done()
            if err := n.sendMessage(targetID, msg); err != nil {
                errors <- fmt.Errorf("failed to sync with %s: %v", targetID, err)
            }
        }(nodeID)
    }

    wg.Wait()
    close(errors)

    var errs []error
    for err := range errors {
        if err != nil {
            errs = append(errs, err)
        }
    }

    if len(errs) > 0 {
        return fmt.Errorf("sync errors: %v", errs)
    }

    return nil
}

// 主函数和命令行处理

func initHydfs(nodeID, address string, port int, isIntroducer bool) (*Node, error) {
    // 设置日志
    logFile := setupLogging(nodeID)
    defer logFile.Close()

    // 创建节点
    node, err := NewNode(nodeID, address, port)
    if err != nil {
        return nil, fmt.Errorf("Failed to create node: %v", err)
    }

    // 设置信号处理
    setupSignalHandler(node)

    // 启动节点
    if err := node.Start(); err != nil {
        return nil, fmt.Errorf("Failed to start node: %v", err)
    }

    // 处理引导节点和加入集群
    if isIntroducer {
        if err := node.StartIntroducer(); err != nil {
            return nil, fmt.Errorf("Failed to start introducer: %v", err)
        }
    } else {
        if err := node.JoinCluster(); err != nil {
            return nil, fmt.Errorf("Failed to join cluster: %v", err)
        }
    }
    return node, nil
}

func handleCommands(n *Node) {
    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        cmd := scanner.Text()
        args := strings.Fields(cmd)
        if len(args) == 0 {
            continue
        }

        switch args[0] {
        case "create":
            if len(args) != 3 {
                fmt.Println("Usage: create <localfilename> <HyDFSfilename>")
                continue
            }
            handleCreate(n, args[1], args[2])

        case "get":
            if len(args) != 3 {
                fmt.Println("Usage: get <HyDFSfilename> <localfilename>")
                continue
            }
            handleGet(n, args[1], args[2])

        case "append":
            if len(args) != 3 {
                fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
                continue
            }
            handleAppend(n, args[1], args[2])

        case "merge":
            if len(args) != 2 {
                fmt.Println("Usage: merge <HyDFSfilename>")
                continue
            }
            handleMerge(n, args[1])

        case "ls":
            if len(args) != 2 {
                fmt.Println("Usage: ls <HyDFSfilename>")
                continue
            }
            handleList(n, args[1])

        case "store":
            handleStore(n)

        case "getfromreplica":
            if len(args) != 4 {
                fmt.Println("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>")
                continue
            }
            handleGetFromReplica(n, args[1], args[2], args[3])

        case "multiappend":
            if len(args) < 4 || len(args)%2 != 0 {
                fmt.Println("Usage: multiappend <HyDFSfilename> <VM1> <localfile1> [<VM2> <localfile2> ...]")
                continue
            }
            handleMultiAppend(n, args[1], args[2:])

        case "list_mem_ids":
            handleListMembershipWithIDs(n)

        case "enable_sus":
            handleEnableSuspicion(n)

        case "disable_sus":
            handleDisableSuspicion(n)

        case "status_sus":
            handleSuspicionStatus(n)

        case "leave":
            handleLeave(n)
            return

        case "measure":
            fmt.Println("Starting measurements... This may take a while...")
            RunAllMeasurements(n)

        case "help":
            printHelp()

        default:
            fmt.Printf("Unknown command: %s\nType 'help' for available commands.\n", args[0])
        }
    }
}

// 命令处理函数
func handleCreate(n *Node, localPath, sdfsPath string) {
    start := time.Now()
    err := n.CreateFile(n.ID, localPath, sdfsPath)
    duration := time.Since(start)

    if err != nil {
        fmt.Printf("Create failed: %v\n", err)
    } else {
        fmt.Printf("Successfully created %s in HyDFS (took %v)\n", sdfsPath, duration)
        // 显示复制位置
        if info, err := n.GetFileInfo(sdfsPath); err == nil {
            fmt.Println("File replicated on nodes:")
            for _, nodeID := range info.ReplicaNodes {
                fmt.Printf("  - %s\n", nodeID)
            }
        }
    }
}

func handleGet(n *Node, sdfsPath, localPath string) {
    start := time.Now()
    err := n.GetFile(n.ID, sdfsPath, localPath)
    duration := time.Since(start)

    if err != nil {
        fmt.Printf("Get failed: %v\n", err)
    } else {
        fmt.Printf("Successfully retrieved %s (took %v)\n", sdfsPath, duration)
    }
}

func handleAppend(n *Node, localPath, sdfsPath string) {
    start := time.Now()
    err := n.AppendFile(n.ID, localPath, sdfsPath)
    duration := time.Since(start)

    if err != nil {
        fmt.Printf("Append failed: %v\n", err)
    } else {
        fmt.Printf("Successfully appended to %s (took %v)\n", sdfsPath, duration)
    }
}

func handleMerge(n *Node, sdfsPath string) {
    start := time.Now()
    err := n.MergeFile(sdfsPath)
    duration := time.Since(start)

    if err != nil {
        fmt.Printf("Merge failed: %v\n", err)
    } else {
        fmt.Printf("Successfully merged %s (took %v)\n", sdfsPath, duration)
    }
}

func handleList(n *Node, sdfsPath string) {
    info, err := n.GetFileInfo(sdfsPath)
    if err != nil {
        fmt.Printf("Failed to get file info: %v\n", err)
        return
    }

    fmt.Printf("\nFile: %s\n", sdfsPath)
    fmt.Printf("Size: %d bytes\n", info.Size)
    fmt.Printf("Version: %d\n", info.Version)
    fmt.Printf("Created: %s\n", info.CreatedAt.Format(time.RFC3339))
    fmt.Printf("Updated: %s\n", info.UpdatedAt.Format(time.RFC3339))
    fmt.Println("\nReplica locations:")
    for i, nodeID := range info.ReplicaNodes {
        fmt.Printf("  %d. %s\n", i+1, nodeID)
    }
}

func handleStore(n *Node) {
    n.mutex.RLock()
    defer n.mutex.RUnlock()

    fmt.Printf("\nNode ID: %s\n", n.ID)
    fmt.Println("Stored files:")
    fmt.Println("-------------")

    if len(n.Files) == 0 {
        fmt.Println("No files stored locally")
        return
    }

    for filename, info := range n.Files {
        fmt.Printf("Filename: %s\n", filename)
        fmt.Printf("  Size: %d bytes\n", info.Size)
        fmt.Printf("  Version: %d\n", info.Version)
        fmt.Printf("  Updated: %s\n", info.UpdatedAt.Format(time.RFC3339))
        fmt.Println("-------------")
    }
}

// 实用函数
func setupLogging(nodeID string) *os.File {
    logDir := "logs"
    if err := os.MkdirAll(logDir, 0755); err != nil {
        fmt.Printf("Failed to create log directory: %v\n", err)
        os.Exit(1)
    }

    logPath := filepath.Join(logDir, fmt.Sprintf("%s.log", nodeID))
    logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        fmt.Printf("Failed to open log file: %v\n", err)
        os.Exit(1)
    }

    multiWriter := io.MultiWriter(os.Stdout, logFile)
    log.SetOutput(multiWriter)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    log.SetPrefix(fmt.Sprintf("[%s] ", nodeID))

    return logFile
}

func setupSignalHandler(n *Node) {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigChan
        fmt.Println("\nReceived shutdown signal, cleaning up...")
        n.Stop()
        os.Exit(0)
    }()
}

func printHelp() {
    fmt.Println("\nAvailable commands:")
    fmt.Println("  create <localfile> <HyDFSfile>   - Create a file in HyDFS")
    fmt.Println("  get <HyDFSfile> <localfile>      - Get a file from HyDFS")
    fmt.Println("  append <localfile> <HyDFSfile>   - Append to a file in HyDFS")
    fmt.Println("  merge <HyDFSfile>                - Merge file replicas")
    fmt.Println("  ls <HyDFSfile>                   - List file information")
    fmt.Println("  store                            - List locally stored files")
    fmt.Println("  getfromreplica <VM> <HDFS> <local> - Get file from specific replica")
    fmt.Println("  multiappend <file> <VM1> <local1>  - Multiple concurrent appends")
    fmt.Println("  list_mem_ids                     - List membership with ring IDs")
    fmt.Println("  enable_sus                       - Enable suspicion mechanism")
    fmt.Println("  disable_sus                      - Disable suspicion mechanism")
    fmt.Println("  status_sus                       - Show suspicion mechanism status")
    fmt.Println("  leave                            - Leave the group")
    fmt.Println("  help                             - Show this help message")
}
