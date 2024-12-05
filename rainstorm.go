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

