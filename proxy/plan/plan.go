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

package plan

import (
	"fmt"
	"github.com/ZzzYtl/MyMask/backend"
	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/parser/ast"
	"github.com/ZzzYtl/MyMask/util"
	"reflect"
	"strings"
)

// type check
var _ Plan = &UnshardPlan{}
var _ Plan = &SelectLastInsertIDPlan{}

// Plan is a interface for select/insert etc.
type Plan interface {
	ExecuteIn(*util.RequestContext, Executor) (*mysql.Result, error)

	// only for cache
	Size() int
}

// Executor TODO: move to package executor
type Executor interface {

	// 执行分片或非分片单条SQL
	ExecuteSQL(ctx *util.RequestContext, slice, db, sql string) (*mysql.Result, error)

	// 执行分片SQL
	ExecuteSQLs(*util.RequestContext, map[string]map[string][]string) ([]*mysql.Result, error)

	// 用于执行INSERT时设置last insert id
	SetLastInsertID(uint64)

	GetLastInsertID() uint64
}

// Checker 用于检查SelectStmt是不是分表的Visitor, 以及是否包含DB信息
type Checker struct {
	db         string
	dbInvalid  bool // SQL是否No database selected
	tableNames []*ast.TableName
}

// NewChecker db为USE db中设置的DB名. 如果没有执行USE db, 则为空字符串
func NewChecker(db string) *Checker {
	return &Checker{
		db:        db,
		dbInvalid: false,
	}
}

func (s *Checker) GetUnshardTableNames() []*ast.TableName {
	return s.tableNames
}

// IsDatabaseInvalid 判断执行计划中是否包含db信息, 如果不包含, 且又含有表名, 则是一个错的执行计划, 应该返回以下错误:
// ERROR 1046 (3D000): No database selected
func (s *Checker) IsDatabaseInvalid() bool {
	return s.dbInvalid
}

// Enter for node visit
func (s *Checker) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch nn := n.(type) {
	case *ast.TableName:
		if s.isTableNameDatabaseInvalid(nn) {
			s.dbInvalid = true
			return n, true
		}

		s.tableNames = append(s.tableNames, nn)
		fmt.Printf("%v \n", reflect.TypeOf(nn))
	default:
		fmt.Printf("%v \n", reflect.TypeOf(nn))
	}

	return n, false
}

// Leave for node visit
func (s *Checker) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, !s.dbInvalid //&& !s.hasShardTable
}

// 如果ast.TableName不带DB名, 且Session未设置DB, 则是不允许的SQL, 应该返回No database selected
func (s *Checker) isTableNameDatabaseInvalid(n *ast.TableName) bool {
	return s.db == "" && n.Schema.L == ""
}

type FieldRelation struct {
	AliasField  string
	AliasTable  string
	OriginField string
	OriginTable string
}

func GetAllFieldsOfTable(table string) []*FieldRelation {
	var rst = make([]*FieldRelation, 0)
	if v, ok := backend.AllTableDesc[table]; ok {
		for _, field := range v {
			rst = append(rst, &FieldRelation{
				AliasField:  field,
				AliasTable:  table,
				OriginField: field,
				OriginTable: table,
			})
		}
	}
	return rst
}

// Checker 用于检查SelectStmt是不是分表的Visitor, 以及是否包含DB信息
type FieldsGettor struct {
	fieldStack       *util.Stack
	curFields        []*FieldRelation
	hasUnSupportFunc bool
}

// NewChecker db为USE db中设置的DB名. 如果没有执行USE db, 则为空字符串
func NewFieldGettor() *FieldsGettor {
	return &FieldsGettor{
		fieldStack:       util.CreateStack(),
		hasUnSupportFunc: false,
	}
}

func (s *FieldsGettor) GetFields() []*FieldRelation {
	return s.fieldStack.Top().([]*FieldRelation)
}

func (s *FieldsGettor) HasUnSupportFunc() bool {
	return s.hasUnSupportFunc
}

func (s *FieldsGettor) AppendFields(fieldsIn ...*FieldRelation) {
	fieldsOut := s.fieldStack.Pop().([]*FieldRelation)
	if fieldsOut != nil {
		fieldsOut = append(fieldsOut, fieldsIn...)
		s.fieldStack.Push(fieldsOut)
	}
}

// Enter for node visit
func (s *FieldsGettor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch nn := n.(type) {
	case *ast.SelectStmt:
		s.fieldStack.Push(make([]*FieldRelation, 0)) //++++++++++++++1
	case *ast.TableSource:
		s.fieldStack.Push(make([]*FieldRelation, 0)) //++++++++++++++2
		//TableName,
		//a SelectStmt, a UnionStmt, or a JoinNode.
		switch mm := nn.Source.(type) {
		case *ast.TableName:
			s.AppendFields(GetAllFieldsOfTable(mm.Name.L)...)
		case *ast.SelectStmt:

		case *ast.UnionStmt:
			//todo
		case *ast.Join:
			//todo
		}
	case *ast.FieldList:
		s.curFields = make([]*FieldRelation, 0)
		s.curFields = append(s.curFields, s.fieldStack.Pop().([]*FieldRelation)...)
		s.fieldStack.Push(make([]*FieldRelation, 0))
	case *ast.WildCardField:
		fields := make([]*FieldRelation, 0)
		for _, v := range s.curFields {
			if len(nn.Table.L) == 0 || strings.EqualFold(nn.Table.L, v.AliasTable) {
				fields = append(fields, v)
			}
		}
		s.AppendFields(fields...)
	case *ast.ColumnNameExpr:
		tableName := nn.Name.Table.L
		colName := nn.Name.Name.L
		for _, v := range s.curFields {
			if (len(nn.Name.Table.L) == 0 || strings.EqualFold(tableName, v.AliasTable)) &&
				strings.EqualFold(colName, v.AliasField) {
				s.AppendFields(v)
				break
			}
		}
	case *ast.OnCondition:
		return n, true
	case *ast.AggregateFuncExpr:
		s.hasUnSupportFunc = true
	case *ast.WindowFuncExpr:
		s.hasUnSupportFunc = true
	case *ast.FuncCallExpr:
		if nn.FnName.O != "CONNECTION_ID" && nn.FnName.O != "NOW" {
			s.hasUnSupportFunc = true
		}
	case *ast.FuncCastExpr:
		s.hasUnSupportFunc = true
	}
	return n, false
}

// Leave for node visit
func (s *FieldsGettor) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch nn := n.(type) {
	case *ast.SelectStmt: //-----------------1
		if s.fieldStack.GetLength() > 1 {
			fields := s.fieldStack.Pop().([]*FieldRelation)
			if fields != nil {
				s.AppendFields(fields...)
			}
		}
	case *ast.TableSource: //------------------2
		tableFields := s.fieldStack.Pop().([]*FieldRelation)
		if len(nn.AsName.L) != 0 {
			for i, _ := range tableFields {
				tableFields[i].AliasTable = nn.AsName.L
			}
		}
		s.AppendFields(tableFields...)
		//a SelectStmt, a UnionStmt, or a JoinNode.
		//switch mm := nn.Source.(type) {
		//case *ast.TableName:
		//case *ast.SelectStmt:
		//case *ast.UnionStmt:
		//	//todo
		//case *ast.Join:
		//	//todo
		//}
	case *ast.SelectField:
		aliasName := nn.AsName.L
		if len(aliasName) != 0 {
			fieldsOut := s.fieldStack.Pop().([]*FieldRelation)
			if fieldsOut != nil {
				len := len(fieldsOut)
				if len > 0 {
					fieldsOut[len-1].AliasField = aliasName
				}
				s.fieldStack.Push(fieldsOut)
			}
		}
	default:
		//fmt.Printf("%v \n", reflect.TypeOf(nn))

	}
	return n, true
}

type basePlan struct{}

func (*basePlan) Size() int {
	return 1
}

// StmtInfo 各种Plan的一些公共属性
type StmtInfo struct {
	db     string // session db
	sql    string // origin sql
	result *RouteResult
}

// TableAliasStmtInfo 使用到表别名, 且依赖表别名做路由计算的StmtNode, 目前包括UPDATE, SELECT
// INSERT也可以使用表别名, 但是由于只存在一个表, 可以直接去掉, 因此不需要.
type TableAliasStmtInfo struct {
	*StmtInfo
	tableAlias map[string]string // key = table alias, value = table
	hintPhyDB  string            // 记录mycat分片时DATABASE()函数指定的物理DB名
}

// BuildPlan build plan for ast
func BuildPlan(stmt ast.StmtNode, phyDBs map[string]string, db, sql string) (Plan, error) {
	if IsSelectLastInsertIDStmt(stmt) {
		return CreateSelectLastInsertIDPlan(), nil
	}

	if estmt, ok := stmt.(*ast.ExplainStmt); ok {
		return buildExplainPlan(estmt, phyDBs, db, sql)
	}

	checker := NewChecker(db)
	stmt.Accept(checker)
	if checker.IsDatabaseInvalid() {
		return nil, fmt.Errorf("no database selected") // TODO: return standard MySQL error
	}
	gettor := NewFieldGettor()
	stmt.Accept(gettor)
	if gettor.HasUnSupportFunc() {
		return nil, fmt.Errorf("has unsupport func")
	}
	fields := gettor.GetFields()
	return CreateUnshardPlan(stmt, phyDBs, db, checker.GetUnshardTableNames(), fields)
}

// NewStmtInfo constructor of StmtInfo
func NewStmtInfo(db string, sql string) *StmtInfo {
	return &StmtInfo{
		db:     db,
		sql:    sql,
		result: NewRouteResult("", "", nil), // nil route result
	}
}

// GetRouteResult get route result
func (s *StmtInfo) GetRouteResult() *RouteResult {
	return s.result
}

func (s *StmtInfo) checkAndGetDB(db string) (string, error) {
	if db != "" && db != s.db {
		return "", fmt.Errorf("db not match")
	}
	return s.db, nil
}

func (t *TableAliasStmtInfo) setTableAlias(table, alias string) error {
	// if not set, set without check
	originTable, ok := t.tableAlias[alias]
	if !ok {
		t.tableAlias[alias] = table
		return nil
	}

	if originTable != table {
		return fmt.Errorf("table alias is set but not match, table: %s, originTable: %s", table, originTable)
	}

	// already set, return
	return nil
}

func (t *TableAliasStmtInfo) getAliasTable(alias string) (string, bool) {
	table, ok := t.tableAlias[alias]
	return table, ok
}
