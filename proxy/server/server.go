// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"fmt"
	"github.com/ZzzYtl/MyMask/log"
	"github.com/ZzzYtl/MyMask/models"
	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/util"
	"github.com/ZzzYtl/MyMask/util/sync2"
	"github.com/howeyc/fsnotify"
)

var (
	timeWheelUnit       = time.Second * 1
	timeWheelBucketsNum = 3600
)

type Listener struct {
	listener net.Listener
	ch       chan interface{}
}

// Server means proxy that serve client request
type Server struct {
	closed    sync2.AtomicBool
	listeners map[uint32]*Listener

	sessionTimeout time.Duration
	tw             *util.TimeWheel
	adminServer    *AdminServer
	manager        *Manager
	cfg            *models.Proxy
	EncryptKey     string
	watcher        *fsnotify.Watcher
	pipe           map[interface{}]chan interface{}
}

// NewServer create new server
func NewServer(cfg *models.Proxy, manager *Manager) (*Server, error) {
	var err error
	s := new(Server)

	// init key
	s.EncryptKey = cfg.EncryptKey
	s.cfg = cfg
	s.manager = manager
	s.pipe = make(map[interface{}]chan interface{}, 64)
	// if error occurs, recycle the resources during creation.
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("NewServer panic: %v", e)
		}

		if err != nil {
			s.Close()
		}
	}()

	s.closed = sync2.NewAtomicBool(false)

	err = s.NewListeners()
	if err != nil {
		return nil, err
	}

	st := strconv.Itoa(cfg.SessionTimeout)
	st = st + "s"
	s.sessionTimeout, err = time.ParseDuration(st)
	if err != nil {
		return nil, err
	}

	s.tw, err = util.NewTimeWheel(timeWheelUnit, timeWheelBucketsNum)
	if err != nil {
		return nil, err
	}
	s.tw.Start()

	// create AdminServer
	adminServer, err := NewAdminServer(s, cfg)
	if err != nil {
		log.Fatal(fmt.Sprintf("NewAdminServer error, quit. error: %s", err.Error()))
		return nil, err
	}
	s.adminServer = adminServer
	s.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("new file watcher fail")
		return nil, err
	}
	err = filepath.Walk(cfg.FileConfigPath, func(path string, info os.FileInfo, err error) error {
		//这里判断是否为目录，只需监控目录即可
		//目录下的文件也在监控范围内，不需要我们一个一个加
		if info.IsDir() {
			path, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			err = s.watcher.Watch(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(fmt.Sprintf("watcher file(%s) error, quit. error: %s", cfg.FileConfigPath, err.Error()))
		return nil, err
	}
	log.Notice("server start succ, netProtoType: %s, addr: %s", cfg.ProtoType, cfg.ProxyAddr)
	return s, nil
}

// Listener return proxy's listener
func (s *Server) Listeners() map[uint32]*Listener {
	return s.listeners
}

func (s *Server) onConn(c net.Conn) {
	cc := newSession(s, c) //新建一个conn
	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			log.Warn("[server] onConn panic error, remoteAddr: %s, stack: %s", c.RemoteAddr().String(), string(buf))
		}

		// close session finally
		cc.Close()
	}()

	if err := cc.Handshake(); err != nil {
		log.Warn("[server] onConn error: %s", err.Error())
		if err != mysql.ErrBadConn {
			cc.c.writeErrorPacket(err)
		}
		return
	}

	// must invoke after handshake
	if allowConnect := cc.IsAllowConnect(); allowConnect == false {
		err := mysql.NewError(mysql.ErrAccessDenied, "ip address access denied by gaea")
		cc.c.writeErrorPacket(err)
		return
	}

	// added into time wheel
	s.tw.Add(s.sessionTimeout, cc, cc.Close)
	s.pipe[cc] = cc.pipe
	cc.Run()
}

// Run proxy run and serve client request
func (s *Server) Run() error {
	// start AdminServer first
	go s.adminServer.Run()
	go s.CheckConfig()
	// start Server
	s.closed.Set(false)
	for _, listener := range s.listeners {
		go func(lis *Listener) {
			for s.closed.Get() != true {
				conn, err := lis.listener.Accept()
				if err != nil {
					log.Warn("[server] listener accept error: %s", err.Error())
					return
				}
				go s.onConn(conn)
			}
		}(listener)
	}
	return nil
}

// Close close proxy server
func (s *Server) Close() error {
	if s.adminServer != nil {
		s.adminServer.Close()
	}

	s.closed.Set(true)
	for _, v := range s.listeners {
		if v.listener != nil {
			err := v.listener.Close()
			if err != nil {
				return err
			}
		}
	}

	s.manager.Close()
	return nil
}

func (s *Server) NewListeners() error {
	s.listeners = make(map[uint32]*Listener)
	ports := s.manager.GetProxyPorts()
	for _, v := range ports {
		s.listeners[v] = &Listener{
			listener: nil,
			ch:       make(chan interface{}),
		}
		listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", v))
		if err != nil {
			return err
		}
		s.listeners[v].listener = listener
	}
	return nil
}

func (s *Server) CheckConfig() {
	for {
		select {
		case <-s.watcher.Event:
			if s.ReloadCfgPrepare() == nil {
				s.ReloadCfgCommit()
				s.CheckListener()
			}
		case err := <-s.watcher.Error:
			log.Warn("error:", err)
		}
	}
}

func (s *Server) ReloadCfgPrepare() error {
	log.Notice("prepare config of all begin")
	namespaceConfigs, err := loadAllNamespace(s.cfg)
	if err != nil {
		log.Warn("reload namespace cfg failed, %v", err)
		return err
	}

	whiteListConfigs, err := loadAllWhiteListConfig(s.cfg)
	if err != nil {
		log.Warn("reload whitelist cfg failed, %v", err)
		return err
	}

	rulesConfigs, err := loadAllRulesConfig(s.cfg)
	if err != nil {
		log.Warn("reload maskrules cfg failed, %v", err)
		return err
	}

	databaseConfigs, err := loadDataBaseConfig(s.cfg)
	if err != nil {
		log.Warn("reload database cfg failed %v", err)
		return err
	}

	err = s.manager.ReloadAllPrepare(namespaceConfigs, whiteListConfigs, rulesConfigs, databaseConfigs)
	if err != nil {
		log.Warn("reload manager error: %v", err)
		return err
	}
	log.Notice("prepare config end")
	return nil
}

func (s *Server) ReloadCfgCommit() error {
	log.Notice("commit config  begin")
	defer func() {
		recover()
	}()
	if err := s.manager.ReloadAllNamespaceCommit(); err != nil {
		log.Warn("Manager ReloadNamespaceCommit error: %v", err)
		return err
	}

	for _, c := range s.pipe {
		c <- nil
	}

	log.Notice("commit config end")
	return nil
}

func (s *Server) CheckListener() {
	ports := s.manager.GetProxyPorts()
	mapPorts := make(map[uint32]bool)
	for _, port := range ports {
		mapPorts[uint32(port)] = true
	}
	for k, v := range s.listeners {
		if _, ok := mapPorts[k]; !ok {
			v.listener.Close()
			delete(s.listeners, k)
		} else {
			delete(mapPorts, k)
		}
	}

	for port, _ := range mapPorts {
		s.listeners[port] = &Listener{
			listener: nil,
			ch:       make(chan interface{}),
		}
		listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			return
		}
		s.listeners[port].listener = listener

		go func(lis *Listener) {
			for s.closed.Get() != true {
				conn, err := lis.listener.Accept()
				if err != nil {
					log.Warn("[server] listener accept error: %s", err.Error())
					return
				}
				go s.onConn(conn)
			}
		}(s.listeners[port])
	}
}

// ReloadNamespacePrepare config change prepare phase
func (s *Server) ReloadNamespacePrepare(name string, client models.Client) error {
	// get namespace conf from etcd
	log.Notice("prepare config of namespace: %s begin", name)
	store := models.NewStore(client)
	namespaceConfig, err := store.LoadNamespace(s.EncryptKey, name)
	if err != nil {
		return err
	}

	if err = s.manager.ReloadNamespacePrepare(namespaceConfig); err != nil {
		log.Warn("Manager ReloadNamespacePrepare error: %v", err)
		return err
	}

	log.Notice("prepare config of namespace: %s end", name)
	return nil
}

// ReloadNamespaceCommit config change commit phase
// commit namespace does not need lock
func (s *Server) ReloadNamespaceCommit(name string) error {
	log.Notice("commit config of namespace: %s begin", name)

	if err := s.manager.ReloadNamespaceCommit(name); err != nil {
		log.Warn("Manager ReloadNamespaceCommit error: %v", err)
		return err
	}

	log.Notice("commit config of namespace: %s end", name)
	return nil
}

// DeleteNamespace delete namespace in namespace manager
func (s *Server) DeleteNamespace(name string) error {
	log.Notice("delete namespace begin: %s", name)

	if err := s.manager.DeleteNamespace(name); err != nil {
		log.Warn("Manager DeleteNamespace error: %v", err)
		return err
	}

	log.Notice("delete namespace end: %s", name)
	return nil
}
