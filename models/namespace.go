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
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/util"
	"github.com/ZzzYtl/MyMask/util/crypto"
)

// Namespace means namespace model stored in etcd
type Namespace struct {
	Name string `json:"name"`
	//Online           bool              `json:"online"`
	//ReadOnly         bool              `json:"read_only"`
	AllowedDBS    map[string]bool   `json:"allowed_dbs"`
	DefaultPhyDBS map[string]string `json:"default_phy_dbs"`
	SlowSQLTime   string            `json:"slow_sql_time"`
	BlackSQL      []string          `json:"black_sql"`
	AllowedIP     []string          `json:"allowed_ip"`
	Slice         *Slice            `json:"db"`
	Users         []*User           `json:"users"` // 客户端接入proxy用户，每个用户可以设置读写分离、读写权限等
	//DefaultSlice     string            `json:"default_slice"`
	//GlobalSequences  []*GlobalSequence `json:"global_sequences"`
	DefaultCharset   string `json:"default_charset"`
	DefaultCollation string `json:"default_collation"`
}

// Encode encode json
func (n *Namespace) Encode() []byte {
	return JSONEncode(n)
}

// Verify verify namespace contents
func (n *Namespace) Verify() error {
	if err := n.verifyName(); err != nil {
		return err
	}

	if err := n.verifyAllowDBS(); err != nil {
		return err
	}

	if err := n.verifyUsers(); err != nil {
		return err
	}

	if err := n.verifySlowSQLTime(); err != nil {
		return err
	}

	if err := n.verifyDBs(); err != nil {
		return err
	}

	if err := n.verifyAllowIps(); err != nil {
		return err
	}

	if err := n.verifyCharset(); err != nil {
		return err
	}

	if err := n.verifySlices(); err != nil {
		return err
	}

	return nil
}

func (n *Namespace) verifyName() error {
	if !n.isNameExists() {
		return fmt.Errorf("must specify namespace name")
	}
	return nil
}

func (n *Namespace) isNameExists() bool {
	return n.Name != ""
}

func (n *Namespace) verifyAllowDBS() error {
	if n.isAllowedDBSEmpty() {
		return errors.New("must specify usable dbs")
	}
	return nil
}

func (n *Namespace) isAllowedDBSEmpty() bool {
	return len(n.AllowedDBS) == 0
}

func (n *Namespace) verifyUsers() error {
	if n.isUsersEmpty() {
		return errors.New("must specify proxy access users")
	}

	for i, u := range n.Users {
		//check namespace
		if u.Namespace == "" {
			u.Namespace = n.Name
		} else if u.Namespace != n.Name {
			return fmt.Errorf("user's namespace name mismatch, user: %s, namespace: %s, %s", u.UserName, n.Name, u.Namespace)
		}

		if err := u.verify(); err != nil {
			return fmt.Errorf("user config error, schema: %s, %v", n.Name, err)
		}

		//check repeat username
		for j := 0; j < i; j++ {
			if n.Users[j].UserName == u.UserName {
				return fmt.Errorf("user duped, namespace: %s, user: %s", n.Name, u.UserName)
			}
		}
	}
	return nil
}

func (n *Namespace) isUsersEmpty() bool {
	return len(n.Users) == 0
}

func (n *Namespace) verifySlowSQLTime() error {
	if !n.isSlowSQLTimeExists() {
		return nil
	}
	if err := n.isSlowSQLTimeValid(); err != nil {
		return err
	}
	return nil
}

func (n *Namespace) isSlowSQLTimeExists() bool {
	return n.SlowSQLTime != ""
}

func (n *Namespace) isSlowSQLTimeValid() error {
	if slowSQLTime, err := strconv.ParseInt(n.SlowSQLTime, 10, 64); err != nil || slowSQLTime < 0 {
		return errors.New("invalid slow sql time")
	}
	return nil
}

func (n *Namespace) verifyDBs() error {
	// no logic database mode
	if n.isDefaultPhyDBSEmpty() {
		return nil
	}

	// logic database mode
	if err := n.isAllowedDBSValid(); err != nil {
		return err
	}
	return nil
}

func (n *Namespace) isDefaultPhyDBSEmpty() bool {
	return len(n.DefaultPhyDBS) == 0
}

func (n *Namespace) isAllowedDBSValid() error {
	for db := range n.AllowedDBS {
		if _, ok := n.DefaultPhyDBS[db]; !ok {
			return fmt.Errorf("db %s have no phy db", db)
		}
	}
	return nil
}

func (n *Namespace) verifyAllowIps() error {
	for _, ipStr := range n.AllowedIP {
		ipStr = strings.TrimSpace(ipStr)
		if len(ipStr) == 0 {
			continue
		}

		if _, err := util.ParseIPInfo(ipStr); err != nil {
			return fmt.Errorf("verify allowips error: %v", err)
		}
	}
	return nil
}

func (n *Namespace) verifyCharset() error {
	if err := mysql.VerifyCharset(n.DefaultCharset, n.DefaultCollation); err != nil {
		return fmt.Errorf("verify charset error: %v", err)
	}
	return nil
}

func (n *Namespace) verifySlices() error {
	if n.isSlicesEmpty() {
		return errors.New("empty slices")
	}
	if err := n.verifyEachSlice(); err != nil {
		return err
	}
	return nil
}

func (n *Namespace) isSlicesEmpty() bool {
	return n.Slice == nil
}

func (n *Namespace) verifyEachSlice() error {
	if n.isSlicesEmpty() {
		return fmt.Errorf("slice cfg error, namespace: %s, slice is nil", n.Name)
	}
	if err := n.Slice.verify(); err != nil {
		return fmt.Errorf("slice cfg error, namespace: %s, err: %s", n.Name, err.Error())
	}
	return nil
}

// Decrypt decrypt user/password in namespace
func (n *Namespace) Decrypt(key string) (err error) {
	return nil
	// Users
	for i := range n.Users {
		n.Users[i].UserName, err = decrypt(key, n.Users[i].UserName)
		if err != nil {
			return
		}
		n.Users[i].Password, err = decrypt(key, n.Users[i].Password)
		if err != nil {
			return
		}
	}
	// Slices
	//for i := range n.Slices {
	//	n.Slices[i].UserName, err = decrypt(key, n.Slices[i].UserName)
	//	if err != nil {
	//		return
	//	}
	//	n.Slices[i].Password, err = decrypt(key, n.Slices[i].Password)
	//	if err != nil {
	//		return
	//	}
	//}

	return nil
}

// Encrypt encrypt user/password in namespace
func (n *Namespace) Encrypt(key string) (err error) {
	//n.IsEncrypt = true
	//// Users
	//for i := range n.Users {
	//	n.Users[i].UserName, err = encrypt(key, n.Users[i].UserName)
	//	if err != nil {
	//		return
	//	}
	//	n.Users[i].Password, err = encrypt(key, n.Users[i].Password)
	//	if err != nil {
	//		return
	//	}
	//}
	//// Slices
	//for i := range n.Slices {
	//	n.Slices[i].UserName, err = encrypt(key, n.Slices[i].UserName)
	//	if err != nil {
	//		return
	//	}
	//	n.Slices[i].Password, err = encrypt(key, n.Slices[i].Password)
	//	if err != nil {
	//		return
	//	}
	//}

	return nil
}

func decrypt(key, data string) (string, error) {
	t, _ := base64.StdEncoding.DecodeString(data)
	origin, err := crypto.DecryptECB(key, t)
	if err != nil {
		return "", err
	}
	return string(origin), nil
}

func encrypt(key, data string) (string, error) {
	tmp, err := crypto.EncryptECB(key, []byte(data))
	if err != nil {
		return "", err
	}
	base64Str := base64.StdEncoding.EncodeToString(tmp)
	return base64Str, nil
}
