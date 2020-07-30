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

package models

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/ZzzYtl/MyMask/log"
	fileclient "github.com/ZzzYtl/MyMask/models/file"
)

// config type
const (
	ConfigFile = "file"
)

// Client client interface
type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	UpdateWithTTL(path string, data []byte, ttl time.Duration) error
	Delete(path string) error
	Read(path string) ([]byte, error)
	List(path string) ([]string, error)
	Close() error
	BasePrefix() string
}

// Store means exported client to use
type Store struct {
	client Client
	prefix string
}

// NewClient constructor to create client by case etcd/file/zk etc.
func NewClient(configType, root string) Client {
	c, err := fileclient.New(root)
	if err != nil {
		log.Warn("create fileclient failed")
		return nil
	}
	return c
}

// NewStore constructor of Store
func NewStore(client Client) *Store {
	return &Store{
		client: client,
		prefix: client.BasePrefix(),
	}
}

// Close close store
func (s *Store) Close() error {
	return s.client.Close()
}

// NamespaceBase return namespace path base
func (s *Store) NamespaceBase() string {
	return filepath.Join(s.prefix, "namespace")
}

// NamespacePath concat namespace path
func (s *Store) NamespacePath(name string) string {
	return filepath.Join(s.prefix, "namespace", name)
}

// ProxyBase return proxy path base
func (s *Store) ProxyBase() string {
	return filepath.Join(s.prefix, "proxy")
}

// ProxyPath concat proxy path
func (s *Store) ProxyPath(token string) string {
	return filepath.Join(s.prefix, "proxy", fmt.Sprintf("proxy-%s", token))
}

// CreateProxy create proxy model
func (s *Store) CreateProxy(p *ProxyInfo) error {
	return s.client.Update(s.ProxyPath(p.Token), p.Encode())
}

// DeleteProxy delete proxy path
func (s *Store) DeleteProxy(token string) error {
	return s.client.Delete(s.ProxyPath(token))
}

// ListNamespace list namespace
func (s *Store) ListNamespace() ([]string, error) {
	files, err := s.client.List(s.NamespaceBase())
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(files); i++ {
		tmp := strings.Split(files[i], "/")
		files[i] = tmp[len(tmp)-1]
	}
	return files, nil
}

// LoadNamespace load namespace value
func (s *Store) LoadNamespace(key, name string) (*Namespace, error) {
	b, err := s.client.Read(s.NamespacePath(name))
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("node %s not exists", s.NamespacePath(name))
	}

	p := &Namespace{}
	if err = json.Unmarshal(b, p); err != nil {
		return nil, err
	}

	if err = p.Verify(); err != nil {
		return nil, err
	}

	if err = p.Decrypt(key); err != nil {
		return nil, err
	}

	return p, nil
}

// UpdateNamespace update namespace path with data
func (s *Store) UpdateNamespace(p *Namespace) error {
	return s.client.Update(s.NamespacePath(p.Name), p.Encode())
}

// DelNamespace delete namespace
func (s *Store) DelNamespace(name string) error {
	return s.client.Delete(s.NamespacePath(name))
}

// ListProxyMonitorMetrics list proxies in proxy register path
func (s *Store) ListProxyMonitorMetrics() (map[string]*ProxyMonitorMetric, error) {
	files, err := s.client.List(s.ProxyBase())
	if err != nil {
		return nil, err
	}
	proxy := make(map[string]*ProxyMonitorMetric)
	for _, path := range files {
		b, err := s.client.Read(path)
		if err != nil {
			return nil, err
		}
		p := &ProxyMonitorMetric{}
		if err := JSONDecode(p, b); err != nil {
			return nil, err
		}
		proxy[p.Token] = p
	}
	return proxy, nil
}

// NamespaceBase return namespace path base
func (s *Store) WhiteListBase() string {
	return filepath.Join(s.prefix, "white_list")
}

// NamespacePath concat namespace path
func (s *Store) WhiteListPath(name string) string {
	return filepath.Join(s.prefix, "white_list", name)
}

// ListNamespace list namespace
func (s *Store) ListWhiteList() ([]string, error) {
	files, err := s.client.List(s.WhiteListBase())
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(files); i++ {
		tmp := strings.Split(files[i], "/")
		files[i] = tmp[len(tmp)-1]
	}
	return files, nil
}

// LoadNamespace load namespace value
func (s *Store) LoadWhiteList(key, name string) (*WhiteList, error) {
	b, err := s.client.Read(s.WhiteListPath(name))
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("node %s not exists", s.WhiteListPath(name))
	}

	p := &WhiteList{Name: name}
	if err = json.Unmarshal(b, &p.Records); err != nil {
		return nil, err
	}

	if err = p.Verify(); err != nil {
		return nil, err
	}

	return p, nil
}

// NamespaceBase return namespace path base
func (s *Store) RuleListBase() string {
	return filepath.Join(s.prefix, "rule")
}

// NamespacePath concat namespace path
func (s *Store) RuleListPath(name string) string {
	return filepath.Join(s.prefix, "rule", name)
}

// LoadNamespace load namespace value
func (s *Store) LoadRuleLists() (*RuleList, error) {
	name := "mysql_rules.xml"
	b, err := s.client.Read(s.RuleListPath(name))
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("node %s not exists", s.RuleListPath(name))
	}
	p := &RuleList{}
	if err = xml.Unmarshal(b, &p); err != nil {
		return nil, err
	}

	if err = p.Verify(); err != nil {
		return nil, err
	}

	return p, nil
}

// LoadNamespace load namespace value
func (s *Store) LoadRule(key, name string) (*FilterList, error) {
	b, err := s.client.Read(s.RuleListPath(name))
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("node %s not exists", s.RuleListPath(name))
	}

	p := &FilterList{}
	if err = xml.Unmarshal(b, &p); err != nil {
		return nil, err
	}

	if err = p.Verify(); err != nil {
		return nil, err
	}

	return p, nil
}

// NamespaceBase return namespace path base
func (s *Store) DBBase() string {
	return filepath.Join(s.prefix, "rule")
}

// NamespacePath concat namespace path
func (s *Store) DBPath(name string) string {
	return filepath.Join(s.prefix, "rule", name)
}

// LoadNamespace load namespace value
func (s *Store) LoadDataBases() ([]DataBase, error) {
	name := "databases.xml"
	b, err := s.client.Read(s.DBPath(name))
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("node %s not exists", s.DBPath(name))
	}
	p := &DataBases{}
	if err = xml.Unmarshal(b, &p); err != nil {
		return nil, err
	}

	if err = p.Verify(); err != nil {
		return nil, err
	}

	return p.DBS, nil
}
