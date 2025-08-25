package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/rand"
)

//------------------------------------------------------------------------------

// FailoverOptions 配置Redis哨兵故障转移客户端的参数选项
// 用于初始化支持自动故障转移的客户端，包含主节点名称、哨兵地址、认证信息等核心配置
type FailoverOptions struct {
	// 主节点名称（在哨兵配置中定义的master名称）
	MasterName string
	// 哨兵节点的地址列表（格式为host:port），用于发现主从节点及监控状态
	SentinelAddrs []string

	// ClientName will execute the `CLIENT SETNAME ClientName` command for each conn.
	ClientName string

	// If specified with SentinelPassword, enables ACL-based authentication (via
	// AUTH <user> <pass>).
	SentinelUsername string
	// Sentinel password from "requirepass <password>" (if enabled) in Sentinel
	// configuration, or, if SentinelUsername is also supplied, used for ACL-based
	// authentication.
	SentinelPassword string

	// Allows routing read-only commands to the closest master or replica node.
	// This option only works with NewFailoverClusterClient.
	RouteByLatency bool
	// Allows routing read-only commands to the random master or replica node.
	// This option only works with NewFailoverClusterClient.
	RouteRandomly bool

	// Route all commands to replica read-only nodes.
	ReplicaOnly bool

	// Use replicas disconnected with master when cannot get connected replicas
	// Now, this option only works in RandomReplicaAddr function.
	UseDisconnectedReplicas bool

	// Following options are copied from Options struct.

	Dialer    func(ctx context.Context, network, addr string) (net.Conn, error)
	OnConnect func(ctx context.Context, cn *Conn) error

	Protocol int
	Username string
	Password string
	DB       int

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	DialTimeout           time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	ContextTimeoutEnabled bool

	PoolFIFO bool

	PoolSize        int
	PoolTimeout     time.Duration
	MinIdleConns    int
	MaxIdleConns    int
	MaxActiveConns  int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration

	TLSConfig *tls.Config

	// DisableIndentity - Disable set-lib on connect.
	//
	// default: false
	//
	// Deprecated: Use DisableIdentity instead.
	DisableIndentity bool

	// DisableIdentity is used to disable CLIENT SETINFO command on connect.
	//
	// default: false
	DisableIdentity bool

	IdentitySuffix string
	UnstableResp3  bool
}

func (opt *FailoverOptions) clientOptions() *Options {
	return &Options{
		Addr:       "FailoverClient",
		ClientName: opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		DB:       opt.DB,
		Protocol: opt.Protocol,
		Username: opt.Username,
		Password: opt.Password,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		DialTimeout:           opt.DialTimeout,
		ReadTimeout:           opt.ReadTimeout,
		WriteTimeout:          opt.WriteTimeout,
		ContextTimeoutEnabled: opt.ContextTimeoutEnabled,

		PoolFIFO:        opt.PoolFIFO,
		PoolSize:        opt.PoolSize,
		PoolTimeout:     opt.PoolTimeout,
		MinIdleConns:    opt.MinIdleConns,
		MaxIdleConns:    opt.MaxIdleConns,
		MaxActiveConns:  opt.MaxActiveConns,
		ConnMaxIdleTime: opt.ConnMaxIdleTime,
		ConnMaxLifetime: opt.ConnMaxLifetime,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:  opt.DisableIdentity,
		DisableIndentity: opt.DisableIndentity,

		IdentitySuffix: opt.IdentitySuffix,
		UnstableResp3:  opt.UnstableResp3,
	}
}

// sentinelOptions 生成与哨兵节点交互的客户端配置
// 参数：addr-哨兵节点地址（host:port）
// 返回：用于连接哨兵节点的Options配置
func (opt *FailoverOptions) sentinelOptions(addr string) *Options {
	return &Options{
		Addr:       addr,
		ClientName: opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		DB:       0,
		Username: opt.SentinelUsername,
		Password: opt.SentinelPassword,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		DialTimeout:           opt.DialTimeout,
		ReadTimeout:           opt.ReadTimeout,
		WriteTimeout:          opt.WriteTimeout,
		ContextTimeoutEnabled: opt.ContextTimeoutEnabled,

		PoolFIFO:        opt.PoolFIFO,
		PoolSize:        opt.PoolSize,
		PoolTimeout:     opt.PoolTimeout,
		MinIdleConns:    opt.MinIdleConns,
		MaxIdleConns:    opt.MaxIdleConns,
		MaxActiveConns:  opt.MaxActiveConns,
		ConnMaxIdleTime: opt.ConnMaxIdleTime,
		ConnMaxLifetime: opt.ConnMaxLifetime,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:  opt.DisableIdentity,
		DisableIndentity: opt.DisableIndentity,

		IdentitySuffix: opt.IdentitySuffix,
		UnstableResp3:  opt.UnstableResp3,
	}
}

// clusterOptions 生成集群客户端配置（用于支持读写分离等特性）
// 返回：集群客户端配置选项
func (opt *FailoverOptions) clusterOptions() *ClusterOptions {
	return &ClusterOptions{
		ClientName: opt.ClientName,

		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		Protocol: opt.Protocol,
		Username: opt.Username,
		Password: opt.Password,

		MaxRedirects: opt.MaxRetries,

		RouteByLatency: opt.RouteByLatency,
		RouteRandomly:  opt.RouteRandomly,

		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		DialTimeout:           opt.DialTimeout,
		ReadTimeout:           opt.ReadTimeout,
		WriteTimeout:          opt.WriteTimeout,
		ContextTimeoutEnabled: opt.ContextTimeoutEnabled,

		PoolFIFO:        opt.PoolFIFO,
		PoolSize:        opt.PoolSize,
		PoolTimeout:     opt.PoolTimeout,
		MinIdleConns:    opt.MinIdleConns,
		MaxIdleConns:    opt.MaxIdleConns,
		MaxActiveConns:  opt.MaxActiveConns,
		ConnMaxIdleTime: opt.ConnMaxIdleTime,
		ConnMaxLifetime: opt.ConnMaxLifetime,

		TLSConfig: opt.TLSConfig,

		DisableIdentity:  opt.DisableIdentity,
		DisableIndentity: opt.DisableIndentity,

		IdentitySuffix: opt.IdentitySuffix,
	}
}

// NewFailoverClient 创建支持自动故障转移的Redis客户端
// 参数：failoverOpt-故障转移配置选项
// 返回：基于哨兵的Redis客户端实例（支持自动切换主节点）
func NewFailoverClient(failoverOpt *FailoverOptions) *Client {
	if failoverOpt == nil {
		panic("redis: NewFailoverClient nil options")
	}

	if failoverOpt.RouteByLatency {
		panic("to route commands by latency, use NewFailoverClusterClient")
	}
	if failoverOpt.RouteRandomly {
		panic("to route commands randomly, use NewFailoverClusterClient")
	}

	sentinelAddrs := make([]string, len(failoverOpt.SentinelAddrs))
	copy(sentinelAddrs, failoverOpt.SentinelAddrs)

	rand.Shuffle(len(sentinelAddrs), func(i, j int) {
		sentinelAddrs[i], sentinelAddrs[j] = sentinelAddrs[j], sentinelAddrs[i]
	})

	failover := &sentinelFailover{
		opt:           failoverOpt,
		sentinelAddrs: sentinelAddrs,
	}

	opt := failoverOpt.clientOptions()
	opt.Dialer = masterReplicaDialer(failover)
	opt.init()

	var connPool *pool.ConnPool

	rdb := &Client{
		baseClient: &baseClient{
			opt: opt,
		},
	}
	rdb.init()

	connPool = newConnPool(opt, rdb.dialHook)
	rdb.connPool = connPool
	rdb.onClose = failover.Close

	failover.mu.Lock()
	failover.onFailover = func(ctx context.Context, addr string) {
		_ = connPool.Filter(func(cn *pool.Conn) bool {
			return cn.RemoteAddr().String() != addr
		})
	}
	failover.mu.Unlock()

	return rdb
}

// masterReplicaDialer 创建主从节点连接的拨号器
// 参数：failover-哨兵故障转移管理器
// 返回：连接拨号函数（根据配置选择主节点或从节点地址建立连接）
func masterReplicaDialer(
	failover *sentinelFailover,
) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var addr string
		var err error

		if failover.opt.ReplicaOnly {
			addr, err = failover.RandomReplicaAddr(ctx)
		} else {
			addr, err = failover.MasterAddr(ctx)
			if err == nil {
				failover.trySwitchMaster(ctx, addr)
			}
		}
		if err != nil {
			return nil, err
		}
		if failover.opt.Dialer != nil {
			return failover.opt.Dialer(ctx, network, addr)
		}

		netDialer := &net.Dialer{
			Timeout:   failover.opt.DialTimeout,
			KeepAlive: 5 * time.Minute,
		}
		if failover.opt.TLSConfig == nil {
			return netDialer.DialContext(ctx, network, addr)
		}
		return tls.DialWithDialer(netDialer, network, addr, failover.opt.TLSConfig)
	}
}

//------------------------------------------------------------------------------

// SentinelClient 用于与Redis哨兵节点交互的客户端
// 提供订阅哨兵消息、获取主节点地址、触发故障转移等操作的方法
type SentinelClient struct {
	*baseClient // 基础客户端（包含连接池、配置等）
	hooksMixin  // 钩子方法混合器（用于扩展处理逻辑）
}

func NewSentinelClient(opt *Options) *SentinelClient {
	if opt == nil {
		panic("redis: NewSentinelClient nil options")
	}
	opt.init()
	c := &SentinelClient{
		baseClient: &baseClient{
			opt: opt,
		},
	}

	c.initHooks(hooks{
		dial:    c.baseClient.dial,
		process: c.baseClient.process,
	})
	c.connPool = newConnPool(opt, c.dialHook)

	return c
}

func (c *SentinelClient) Process(ctx context.Context, cmd Cmder) error {
	err := c.processHook(ctx, cmd)
	cmd.SetErr(err)
	return err
}

func (c *SentinelClient) pubSub() *PubSub {
	pubsub := &PubSub{
		opt: c.opt,

		newConn: func(ctx context.Context, channels []string) (*pool.Conn, error) {
			return c.newConn(ctx)
		},
		closeConn: c.connPool.CloseConn,
	}
	pubsub.init()
	return pubsub
}

// Ping 测试与哨兵节点的连接是否存活或测量延迟
// 参数：ctx-上下文
// 返回：字符串命令对象（响应结果为"PONG"表示连接正常）
func (c *SentinelClient) Ping(ctx context.Context) *StringCmd {
	cmd := NewStringCmd(ctx, "ping")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Subscribe 订阅指定的哨兵频道（如"+switch-master"）
// 参数：ctx-上下文，channels-要订阅的频道列表
// 返回：发布订阅客户端实例（用于接收频道消息）
func (c *SentinelClient) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *SentinelClient) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

// GetMasterAddrByName 通过主节点名称获取当前主节点的地址
// 参数：ctx-上下文，name-主节点名称（对应Sentinel配置中的master名称）
// 返回：主节点地址（格式为[host, port]）的命令对象
func (c *SentinelClient) GetMasterAddrByName(ctx context.Context, name string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "sentinel", "get-master-addr-by-name", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *SentinelClient) Sentinels(ctx context.Context, name string) *MapStringStringSliceCmd {
	cmd := NewMapStringStringSliceCmd(ctx, "sentinel", "sentinels", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Failover 强制触发主节点故障转移（不等待其他哨兵确认）
// 参数：ctx-上下文，name-主节点名称
// 返回：状态命令对象（执行结果）
func (c *SentinelClient) Failover(ctx context.Context, name string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "sentinel", "failover", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Reset 重置匹配名称的主节点状态（清除故障转移进度、关联的从节点和哨兵信息）
// 参数：ctx-上下文，pattern-主节点名称的glob模式（如"mymaster*"）
// 返回：整数命令对象（重置的主节点数量）
func (c *SentinelClient) Reset(ctx context.Context, pattern string) *IntCmd {
	cmd := NewIntCmd(ctx, "sentinel", "reset", pattern)
	_ = c.Process(ctx, cmd)
	return cmd
}

// FlushConfig 强制哨兵将当前状态（如监控的主节点信息）写入磁盘配置文件
// 参数：ctx-上下文
// 返回：状态命令对象（执行结果）
func (c *SentinelClient) FlushConfig(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "sentinel", "flushconfig")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Master 获取指定主节点的详细状态信息（如运行状态、从节点数量等）
// 参数：ctx-上下文，name-主节点名称
// 返回：字符串映射命令对象（键值对形式的状态信息）
func (c *SentinelClient) Master(ctx context.Context, name string) *MapStringStringCmd {
	cmd := NewMapStringStringCmd(ctx, "sentinel", "master", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Masters 获取所有被哨兵监控的主节点列表及其状态
// 参数：ctx-上下文
// 返回：切片命令对象（每个元素为主节点状态信息）
func (c *SentinelClient) Masters(ctx context.Context) *SliceCmd {
	cmd := NewSliceCmd(ctx, "sentinel", "masters")
	_ = c.Process(ctx, cmd)
	return cmd
}

// Replicas 获取指定主节点的从节点列表及其状态（如连接状态、复制偏移量等）
// 参数：ctx-上下文，name-主节点名称
// 返回：字符串映射字符串切片命令对象（从节点信息）
func (c *SentinelClient) Replicas(ctx context.Context, name string) *MapStringStringSliceCmd {
	cmd := NewMapStringStringSliceCmd(ctx, "sentinel", "replicas", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// CkQuorum 检查当前哨兵配置是否满足故障转移的法定人数和多数同意条件
// 参数：ctx-上下文，name-主节点名称
// 返回：字符串命令对象（结果包含"OK"或具体错误信息）
func (c *SentinelClient) CkQuorum(ctx context.Context, name string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "ckquorum", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Monitor 配置哨兵开始监控一个新的主节点
// 参数：ctx-上下文，name-主节点名称，ip-主节点IP，port-主节点端口，quorum-故障转移所需的最小哨兵数量
// 返回：字符串命令对象（执行结果）
func (c *SentinelClient) Monitor(ctx context.Context, name, ip, port, quorum string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "monitor", name, ip, port, quorum)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Set 修改指定主节点的配置参数（如故障转移超时时间、监控权重等）
// 参数：ctx-上下文，name-主节点名称，option-配置项名称，value-配置项值
// 返回：字符串命令对象（执行结果）
func (c *SentinelClient) Set(ctx context.Context, name, option, value string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "set", name, option, value)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Remove 停止哨兵对指定主节点的监控，并从内部状态中移除该主节点
// 参数：ctx-上下文，name-主节点名称
// 返回：字符串命令对象（执行结果）
func (c *SentinelClient) Remove(ctx context.Context, name string) *StringCmd {
	cmd := NewStringCmd(ctx, "sentinel", "remove", name)
	_ = c.Process(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// sentinelFailover 管理哨兵故障转移的核心逻辑结构体
// 负责与哨兵节点交互、监控主节点状态、触发故障转移等操作
type sentinelFailover struct {
	opt *FailoverOptions // 故障转移配置选项

	sentinelAddrs []string // 哨兵节点地址列表

	onFailover func(ctx context.Context, addr string) // 主节点切换时的回调函数
	onUpdate   func(ctx context.Context)              // 状态更新时的回调函数

	mu          sync.RWMutex    // 读写锁（保护共享状态）
	_masterAddr string          // 当前主节点地址（受锁保护）
	sentinel    *SentinelClient // 哨兵客户端实例
	pubsub      *PubSub         // 用于订阅哨兵消息的发布订阅客户端
}

// Close 关闭哨兵故障转移管理器（释放连接资源）
// 返回：关闭过程中出现的第一个错误（无则返回nil）
func (c *sentinelFailover) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sentinel != nil {
		return c.closeSentinel()
	}
	return nil
}

func (c *sentinelFailover) closeSentinel() error {
	firstErr := c.pubsub.Close()
	c.pubsub = nil

	err := c.sentinel.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	c.sentinel = nil

	return firstErr
}

func (c *sentinelFailover) RandomReplicaAddr(ctx context.Context) (string, error) {
	if c.opt == nil {
		return "", errors.New("opt is nil")
	}

	addresses, err := c.replicaAddrs(ctx, false)
	if err != nil {
		return "", err
	}

	if len(addresses) == 0 && c.opt.UseDisconnectedReplicas {
		addresses, err = c.replicaAddrs(ctx, true)
		if err != nil {
			return "", err
		}
	}

	if len(addresses) == 0 {
		return c.MasterAddr(ctx)
	}
	return addresses[rand.Intn(len(addresses))], nil
}

func (c *sentinelFailover) MasterAddr(ctx context.Context) (string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()

	if sentinel != nil {
		addr, err := c.getMasterAddr(ctx, sentinel)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return "", err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName name=%q failed: %s",
				c.opt.MasterName, err)
		} else {
			return addr, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sentinel != nil {
		addr, err := c.getMasterAddr(ctx, c.sentinel)
		if err != nil {
			_ = c.closeSentinel()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return "", err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName name=%q failed: %s",
				c.opt.MasterName, err)
		} else {
			return addr, nil
		}
	}

	var (
		masterAddr string
		wg         sync.WaitGroup
		once       sync.Once
		errCh      = make(chan error, len(c.sentinelAddrs))
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, sentinelAddr := range c.sentinelAddrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			sentinelCli := NewSentinelClient(c.opt.sentinelOptions(addr))
			addrVal, err := sentinelCli.GetMasterAddrByName(ctx, c.opt.MasterName).Result()
			if err != nil {
				internal.Logger.Printf(ctx, "sentinel: GetMasterAddrByName addr=%s, master=%q failed: %s",
					addr, c.opt.MasterName, err)
				_ = sentinelCli.Close()
				errCh <- err
				return
			}
			once.Do(func() {
				masterAddr = net.JoinHostPort(addrVal[0], addrVal[1])
				// Push working sentinel to the top
				c.sentinelAddrs[0], c.sentinelAddrs[i] = c.sentinelAddrs[i], c.sentinelAddrs[0]
				c.setSentinel(ctx, sentinelCli)
				internal.Logger.Printf(ctx, "sentinel: selected addr=%s masterAddr=%s", addr, masterAddr)
				cancel()
			})
		}(i, sentinelAddr)
	}

	wg.Wait()
	close(errCh)
	if masterAddr != "" {
		return masterAddr, nil
	}
	errs := make([]error, 0, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}
	return "", fmt.Errorf("redis: all sentinels specified in configuration are unreachable: %w", errors.Join(errs...))
}

func (c *sentinelFailover) replicaAddrs(ctx context.Context, useDisconnected bool) ([]string, error) {
	c.mu.RLock()
	sentinel := c.sentinel
	c.mu.RUnlock()

	if sentinel != nil {
		addrs, err := c.getReplicaAddrs(ctx, sentinel)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
				c.opt.MasterName, err)
		} else if len(addrs) > 0 {
			return addrs, nil
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sentinel != nil {
		addrs, err := c.getReplicaAddrs(ctx, c.sentinel)
		if err != nil {
			_ = c.closeSentinel()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			// Continue on other errors
			internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
				c.opt.MasterName, err)
		} else if len(addrs) > 0 {
			return addrs, nil
		} else {
			// No error and no replicas.
			_ = c.closeSentinel()
		}
	}

	var sentinelReachable bool

	for i, sentinelAddr := range c.sentinelAddrs {
		sentinel := NewSentinelClient(c.opt.sentinelOptions(sentinelAddr))

		replicas, err := sentinel.Replicas(ctx, c.opt.MasterName).Result()
		if err != nil {
			_ = sentinel.Close()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			internal.Logger.Printf(ctx, "sentinel: Replicas master=%q failed: %s",
				c.opt.MasterName, err)
			continue
		}
		sentinelReachable = true
		addrs := parseReplicaAddrs(replicas, useDisconnected)
		if len(addrs) == 0 {
			continue
		}
		// Push working sentinel to the top.
		c.sentinelAddrs[0], c.sentinelAddrs[i] = c.sentinelAddrs[i], c.sentinelAddrs[0]
		c.setSentinel(ctx, sentinel)

		return addrs, nil
	}

	if sentinelReachable {
		return []string{}, nil
	}
	return []string{}, errors.New("redis: all sentinels specified in configuration are unreachable")
}

func (c *sentinelFailover) getMasterAddr(ctx context.Context, sentinel *SentinelClient) (string, error) {
	addr, err := sentinel.GetMasterAddrByName(ctx, c.opt.MasterName).Result()
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(addr[0], addr[1]), nil
}

func (c *sentinelFailover) getReplicaAddrs(ctx context.Context, sentinel *SentinelClient) ([]string, error) {
	addrs, err := sentinel.Replicas(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Replicas name=%q failed: %s",
			c.opt.MasterName, err)
		return nil, err
	}
	return parseReplicaAddrs(addrs, false), nil
}

func parseReplicaAddrs(addrs []map[string]string, keepDisconnected bool) []string {
	nodes := make([]string, 0, len(addrs))
	for _, node := range addrs {
		isDown := false
		if flags, ok := node["flags"]; ok {
			for _, flag := range strings.Split(flags, ",") {
				switch flag {
				case "s_down", "o_down":
					isDown = true
				case "disconnected":
					if !keepDisconnected {
						isDown = true
					}
				}
			}
		}
		if !isDown && node["ip"] != "" && node["port"] != "" {
			nodes = append(nodes, net.JoinHostPort(node["ip"], node["port"]))
		}
	}

	return nodes
}

// trySwitchMaster 尝试切换当前主节点地址（触发主节点故障转移后的地址更新）
// 参数：ctx-上下文，addr-新的主节点地址（格式为host:port）
func (c *sentinelFailover) trySwitchMaster(ctx context.Context, addr string) {
	c.mu.RLock()
	currentAddr := c._masterAddr //nolint:ifshort
	c.mu.RUnlock()

	if addr == currentAddr {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if addr == c._masterAddr {
		return
	}
	c._masterAddr = addr

	internal.Logger.Printf(ctx, "sentinel: new master=%q addr=%q",
		c.opt.MasterName, addr)
	if c.onFailover != nil {
		c.onFailover(ctx, addr)
	}
}

func (c *sentinelFailover) setSentinel(ctx context.Context, sentinel *SentinelClient) {
	if c.sentinel != nil {
		panic("not reached")
	}
	c.sentinel = sentinel
	c.discoverSentinels(ctx)

	c.pubsub = sentinel.Subscribe(ctx, "+switch-master", "+replica-reconf-done")
	go c.listen(c.pubsub)
}

func (c *sentinelFailover) discoverSentinels(ctx context.Context) {
	sentinels, err := c.sentinel.Sentinels(ctx, c.opt.MasterName).Result()
	if err != nil {
		internal.Logger.Printf(ctx, "sentinel: Sentinels master=%q failed: %s", c.opt.MasterName, err)
		return
	}
	for _, sentinel := range sentinels {
		ip, ok := sentinel["ip"]
		if !ok {
			continue
		}
		port, ok := sentinel["port"]
		if !ok {
			continue
		}
		if ip != "" && port != "" {
			sentinelAddr := net.JoinHostPort(ip, port)
			if !contains(c.sentinelAddrs, sentinelAddr) {
				internal.Logger.Printf(ctx, "sentinel: discovered new sentinel=%q for master=%q",
					sentinelAddr, c.opt.MasterName)
				c.sentinelAddrs = append(c.sentinelAddrs, sentinelAddr)
			}
		}
	}
}

func (c *sentinelFailover) listen(pubsub *PubSub) {
	ctx := context.TODO()

	if c.onUpdate != nil {
		c.onUpdate(ctx)
	}

	ch := pubsub.Channel()
	for msg := range ch {
		if msg.Channel == "+switch-master" {
			parts := strings.Split(msg.Payload, " ")
			if parts[0] != c.opt.MasterName {
				internal.Logger.Printf(pubsub.getContext(), "sentinel: ignore addr for master=%q", parts[0])
				continue
			}
			addr := net.JoinHostPort(parts[3], parts[4])
			c.trySwitchMaster(pubsub.getContext(), addr)
		}

		if c.onUpdate != nil {
			c.onUpdate(ctx)
		}
	}
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------

// NewFailoverClusterClient returns a client that supports routing read-only commands
// to a replica node.
func NewFailoverClusterClient(failoverOpt *FailoverOptions) *ClusterClient {
	if failoverOpt == nil {
		panic("redis: NewFailoverClusterClient nil options")
	}

	sentinelAddrs := make([]string, len(failoverOpt.SentinelAddrs))
	copy(sentinelAddrs, failoverOpt.SentinelAddrs)

	failover := &sentinelFailover{
		opt:           failoverOpt,
		sentinelAddrs: sentinelAddrs,
	}

	opt := failoverOpt.clusterOptions()
	if failoverOpt.DB != 0 {
		onConnect := opt.OnConnect

		opt.OnConnect = func(ctx context.Context, cn *Conn) error {
			if err := cn.Select(ctx, failoverOpt.DB).Err(); err != nil {
				return err
			}

			if onConnect != nil {
				return onConnect(ctx, cn)
			}

			return nil
		}
	}

	opt.ClusterSlots = func(ctx context.Context) ([]ClusterSlot, error) {
		masterAddr, err := failover.MasterAddr(ctx)
		if err != nil {
			return nil, err
		}

		nodes := []ClusterNode{{
			Addr: masterAddr,
		}}

		replicaAddrs, err := failover.replicaAddrs(ctx, false)
		if err != nil {
			return nil, err
		}

		for _, replicaAddr := range replicaAddrs {
			nodes = append(nodes, ClusterNode{
				Addr: replicaAddr,
			})
		}

		slots := []ClusterSlot{
			{
				Start: 0,
				End:   16383,
				Nodes: nodes,
			},
		}
		return slots, nil
	}

	c := NewClusterClient(opt)

	failover.mu.Lock()
	failover.onUpdate = func(ctx context.Context) {
		c.ReloadState(ctx)
	}
	failover.mu.Unlock()

	return c
}
