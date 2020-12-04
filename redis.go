package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/mediocregopher/radix"
	"golang.org/x/net/proxy"
)

type Socks5ProxyConfig struct {
	User string `json:"user"`
	Pass string `json:"pass"`
	Addr string `json:"addr"`
}

// json config example:
// {
// 	"redis-standalone": [
// 		{
// 			"tag": "s1",
// 			"addr": "127.0.0.1:6379",
// 			"timeout": 2000,
// 			"pool_size": 20,
//			"socks5":{"user","u", "pass":"p", "addr":"127.0.0.1:8888"}
// 		}
// 	],
// 	"redis-sentinel": [
// 		{
// 			"master_tag": {"master1":"tag1", "master2":"tag2"},
// 			"addrs": ["127.0.0.1:26379","127.0.0.2:26379"],
// 			"timeout": 1000,
// 			"pool_size": 20
// 		}
// 	],
// 	"redis-cluster": [
// 		{
// 			"tag": "c1",
// 			"addrs": ["127.0.0.1:7000","127.0.0.2:7001"],
// 			"timeout": 1000,
// 			"pool_size": 20
// 		}
// 	]
// }
//
type StandaloneConfig struct {
	Tag      string            `json:"tag"`
	Addr     string            `json:"addr"`
	Timeout  int               `json:"timeout"`
	PoolSize int               `json:"pool_size"`
	Socks5   Socks5ProxyConfig `json:"socks5"`
}

type SentinelConfig struct {
	MasterTag map[string]string `json:"master_tag"`
	Addrs     []string          `json:"addrs"`
	Timeout   int               `json:"timeout"`
	PoolSize  int               `json:"pool_size"`
	Socks5    Socks5ProxyConfig `json:"socks5"`
}

type ClusterConfig struct {
	Tag      string            `json:"tag"`
	Addrs    []string          `json:"addrs"`
	Timeout  int               `json:"timeout"`
	PoolSize int               `json:"pool_size"`
	Socks5   Socks5ProxyConfig `json:"socks5"`
}

type ConfigWrapper struct {
	StandCfg    []StandaloneConfig `json:"redis-standalone"`
	SentinelCfg []SentinelConfig   `json:"redis-sentinel"`
	ClusterCfg  []ClusterConfig    `json:"redis-cluster"`
}

type LuaScript struct {
	Script, SHA string
}

// key - tag
// value - client
var clientMap sync.Map

var defaultTimeout = 3000
var defaultPoolSize = 10

var logStdout = func(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}
var logWarn = logStdout
var logInfo = logStdout

func InitRedisStandalone(cfg []StandaloneConfig) error {
	for _, c := range cfg {
		var timeout, poolSize = defaultTimeout, defaultPoolSize
		if c.Timeout > 0 {
			timeout = c.Timeout
		}
		if c.PoolSize > 0 {
			poolSize = c.PoolSize
		}

		customConnFunc := func(network, addr string) (radix.Conn, error) {
			if len(c.Socks5.Addr) > 0 {
				auth := &proxy.Auth{User: c.Socks5.User, Password: c.Socks5.Pass}
				pd, err := proxy.SOCKS5("tcp", c.Socks5.Addr, auth, nil)
				if err != nil {
					panic(err)
				}
				dailer := func(addr string) (net.Conn, error) {
					return pd.Dial("tcp", addr)
				}
				conn, err := dailer(addr)
				return radix.NewConn(conn), err
			}
			return radix.Dial(network, addr, radix.DialTimeout(time.Duration(timeout)*time.Millisecond))
		}

		client, err := radix.NewPool("tcp", c.Addr, poolSize, radix.PoolConnFunc(customConnFunc))
		if err != nil {
			return err
		}

		clientMap.Store(c.Tag, client)
		logInfo("redis.InitRedisStandalone with %+v", c)
	}
	return nil
}

func InitRedisSentinel(cfg []SentinelConfig) error {
	for _, c := range cfg {
		var timeout, poolSize = defaultTimeout, defaultPoolSize
		if c.Timeout > 0 {
			timeout = c.Timeout
		}
		if c.PoolSize > 0 {
			poolSize = c.PoolSize
		}

		customConnFunc := func(network, addr string) (radix.Conn, error) {
			if len(c.Socks5.Addr) > 0 {
				auth := &proxy.Auth{User: c.Socks5.User, Password: c.Socks5.Pass}
				pd, err := proxy.SOCKS5("tcp", c.Socks5.Addr, auth, nil)
				if err != nil {
					panic(err)
				}
				dailer := func(addr string) (net.Conn, error) {
					return pd.Dial("tcp", addr)
				}
				conn, err := dailer(addr)
				return radix.NewConn(conn), err
			}
			return radix.Dial(network, addr, radix.DialTimeout(time.Duration(timeout)*time.Millisecond))
		}

		customClientFunc := func(network, addr string) (radix.Client, error) {
			return radix.NewPool(network, addr, poolSize)
		}

		for mastername, tag := range c.MasterTag {
			client, err := radix.NewSentinel(mastername, c.Addrs,
				radix.SentinelConnFunc(customConnFunc), radix.SentinelPoolFunc(customClientFunc))
			if err != nil {
				return err
			}

			clientMap.Store(tag, client)
			logInfo("redis.InitRedisSentinel with %+v", c)
		}
	}
	return nil
}

func InitRedisCluster(cfg []ClusterConfig) error {
	for _, c := range cfg {
		var timeout, poolSize = defaultTimeout, defaultPoolSize
		if c.Timeout > 0 {
			timeout = c.Timeout
		}
		if c.PoolSize > 0 {
			poolSize = c.PoolSize
		}

		customConnFunc := func(network, addr string) (radix.Conn, error) {
			if len(c.Socks5.Addr) > 0 {
				auth := &proxy.Auth{User: c.Socks5.User, Password: c.Socks5.Pass}
				pd, err := proxy.SOCKS5("tcp", c.Socks5.Addr, auth, nil)
				if err != nil {
					panic(err)
				}
				dailer := func(addr string) (net.Conn, error) {
					return pd.Dial("tcp", addr)
				}
				conn, err := dailer(addr)
				return radix.NewConn(conn), err
			}
			return radix.Dial(network, addr, radix.DialTimeout(time.Duration(timeout)*time.Millisecond))
		}

		customClientFunc := func(network, addr string) (radix.Client, error) {
			return radix.NewPool(network, addr, poolSize, radix.PoolConnFunc(customConnFunc))
		}

		client, err := radix.NewCluster(c.Addrs, radix.ClusterPoolFunc(customClientFunc))
		if err != nil {
			return nil
		}
		clientMap.Store(c.Tag, client)
		logInfo("redis.InitRedisCluster with %+v", c)
	}
	return nil
}

func InitWith(filename string) error {
	if len(filename) == 0 {
		filename = "../conf/server.json"
	}

	var cfgs ConfigWrapper
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(raw, &cfgs)
	if err != nil {
		return err
	}

	logInfo("redis.InitWith %+v", cfgs)
	if len(cfgs.StandCfg) > 0 {
		err = InitRedisStandalone(cfgs.StandCfg)
		if err != nil {
			return err
		}
	}
	if len(cfgs.SentinelCfg) > 0 {
		err = InitRedisSentinel(cfgs.SentinelCfg)
		if err != nil {
			return err
		}
	}
	if len(cfgs.ClusterCfg) > 0 {
		err = InitRedisCluster(cfgs.ClusterCfg)
		if err != nil {
			return err
		}
	}

	return nil
}

func SetLogInfoFunc(f func(format string, a ...interface{})) {
	logInfo = f
}

func Destory() {
	clientMap.Range(func(k, v interface{}) bool {
		client := v.(radix.Client)
		client.Close()
		return true
	})
}

func getClientByTag(tag string) (radix.Client, error) {
	if c, ok := clientMap.Load(tag); ok {
		var err error = nil
		var client radix.Client = nil
		switch cc := c.(type) {
		case radix.Client:
			client, err = cc, nil
		default:
			client, err = nil, errors.New("Client Type err!")
		}

		return client, err
	}
	return nil, fmt.Errorf("Can not find client with tag [%s]", tag)
}

func GetRadixClient(tag string) (radix.Client, error) {
	return getClientByTag(tag)
}

func Do(rcv interface{}, tag, cmd, key string, args ...interface{}) error {
	t := time.Now()
	defer func() {
		t2 := time.Since(t)
		r := reflect.ValueOf(rcv).Elem()
		logInfo("redis.Do cost:%v tag:%s cmd:%s key:%s rcv:%#v", t2, tag, cmd, key, r)
	}()

	client, err := getClientByTag(tag)
	if err == nil {
		return client.Do(radix.FlatCmd(rcv, cmd, key, args...))
	}
	return err
}

func DoCmd(rcv interface{}, tag, cmd string, args ...string) error {
	t := time.Now()
	defer func() {
		t2 := time.Since(t)
		r := reflect.ValueOf(rcv).Elem()
		logInfo("redis.DoCmd cost:%v tag:%s cmd:%s rcv:%#v", t2, tag, cmd, r)
	}()

	client, err := getClientByTag(tag)
	if err == nil {
		return client.Do(radix.Cmd(rcv, cmd, args...))
	}
	return err
}

func Eval(rcv interface{}, tag, script string, numKeys int, args ...string) error {
	t := time.Now()
	defer func() {
		t2 := time.Since(t)
		r := reflect.ValueOf(rcv).Elem()
		logInfo("redis.Eval cost:%v tag:%s script:%s rcv:%#v", t2, tag, script, r)
	}()

	client, err := getClientByTag(tag)
	if err == nil {
		var s = radix.NewEvalScript(numKeys, script)
		return client.Do(s.Cmd(rcv, args...))
	}
	return err
}

func EvalSmart(rcv interface{}, tag string, script *LuaScript, numKeys int, args ...string) error {
	t := time.Now()
	defer func() {
		t2 := time.Since(t)
		r := reflect.ValueOf(rcv).Elem()
		logInfo("redis.EvalSmart cost:%v tag:%s lua_sha:%s rcv:%#v", t2, tag, script.SHA, r)
	}()

	client, err := getClientByTag(tag)
	if err != nil {
		return err
	}

	if len(script.SHA) == 0 {
		var ret string
		err = client.Do(radix.Cmd(&ret, "SCRIPT", "LOAD", script.Script))
		if err != nil {
			return err
		}
		script.SHA = ret
	}

	var realArgs = make([]string, 0, len(args)+2)
	realArgs = append(realArgs, script.SHA, strconv.FormatInt(int64(numKeys), 10))
	realArgs = append(realArgs, args...)
	return client.Do(radix.Cmd(rcv, "EVALSHA", realArgs...))
}
