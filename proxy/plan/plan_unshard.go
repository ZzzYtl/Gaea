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
	"github.com/ZzzYtl/MyMask/parser/model"
	"strings"

	"github.com/ZzzYtl/MyMask/backend"
	"github.com/ZzzYtl/MyMask/mysql"
	"github.com/ZzzYtl/MyMask/parser/ast"
	"github.com/ZzzYtl/MyMask/parser/format"
	"github.com/ZzzYtl/MyMask/util"
)

// UnshardPlan is the plan for unshard statement
type UnshardPlan struct {
	basePlan

	db     string
	phyDBs map[string]string
	sql    string
	stmt   ast.StmtNode
}

// SelectLastInsertIDPlan is the plan for SELECT LAST_INSERT_ID()
// TODO: fix below
// https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
// The value of LAST_INSERT_ID() is not changed if you set the AUTO_INCREMENT column of a row
// to a non-“magic” value (that is, a value that is not NULL and not 0).
type SelectLastInsertIDPlan struct {
	basePlan
}

// IsSelectLastInsertIDStmt check if the statement is SELECT LAST_INSERT_ID()
func IsSelectLastInsertIDStmt(stmt ast.StmtNode) bool {
	s, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return false
	}

	if len(s.Fields.Fields) != 1 {
		return false
	}

	if s.From != nil || s.Where != nil || s.GroupBy != nil || s.Having != nil || s.OrderBy != nil || s.Limit != nil {
		return false
	}

	f, ok := s.Fields.Fields[0].Expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}

	return f.FnName.L == "last_insert_id"
}

func ProcessMask(n *ast.SelectStmt, fieldRealstions *[]*FieldRelation) {
	if n.Fields == nil {
		return
	}

	fields := make([]*ast.SelectField, 0)
	for _, field := range n.Fields.Fields {
		newFields := ProcessSelectFieldMask(field, fieldRealstions)
		fields = append(fields, newFields...)
	}
	n.Fields.Fields = fields
}

func ProcessFieldMask(n ast.Node, fieldsRelation *[]*FieldRelation) ast.Node {
	switch v := n.(type) {
	case *ast.FuncCallExpr:
		if strings.EqualFold(v.FnName.L, "left") {
			return v
		} else {
			ProcessArgsMask(v.Args, fieldsRelation)
			return v
		}
	case *ast.ColumnNameExpr:
		if maskFunc, ok := IsMaskField(v.Name, fieldsRelation); ok {
			return PackMaskNode(v, maskFunc)
		}
		return v
	}
	return n
}

func ProcessSelectFieldMask(field *ast.SelectField, fieldRealstions *[]*FieldRelation) []*ast.SelectField {
	var fields []*ast.SelectField
	if field.Expr != nil {
		if len(*fieldRealstions) > 0 {
			var str = ""
			if len((*fieldRealstions)[0].AliasTable) > 0 {
				str = (*fieldRealstions)[0].AliasTable + "." + (*fieldRealstions)[0].AliasField
			} else {
				str = (*fieldRealstions)[0].AliasField
			}
			field.AsName.L = str
			field.AsName.O = str
		}
		field.Expr = ProcessFieldMask(field.Expr, fieldRealstions).(ast.ExprNode)
		fields = append(fields, field)
		return fields
	} else if field.WildCard != nil {
		for _, v := range *fieldRealstions {
			if len(field.WildCard.Table.L) == 0 || strings.EqualFold(v.AliasTable, field.WildCard.Table.L) {
				newField := &ast.SelectField{}
				if len(*fieldRealstions) > 0 {
					var str = ""
					if len((*fieldRealstions)[0].AliasTable) > 0 {
						str = (*fieldRealstions)[0].AliasTable + "." + (*fieldRealstions)[0].AliasField
					} else {
						str = (*fieldRealstions)[0].AliasField
					}
					field.AsName.L = str
					field.AsName.O = str
				}
				newField.Expr = &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Table: model.CIStr{v.AliasTable, strings.ToLower(v.AliasTable)},
						Name:  model.CIStr{v.AliasField, strings.ToLower(v.AliasField)},
					},
				}
				newFields := ProcessSelectFieldMask(newField, fieldRealstions)
				fields = append(fields, newFields...)
			}
		}
	}
	return fields
}

func IsMaskField(Name *ast.ColumnName, fields *[]*FieldRelation) (string, bool) {
	if len(*fields) > 0 {
		funC := (*fields)[0].MaskFunc
		isMaskField := (*fields)[0].IsMaskField
		(*fields) = (*fields)[1:]
		return funC, isMaskField
	}
	return "", false
	//for _, v := range fields {
	//	if strings.EqualFold(Name.Name.L, v.AliasField) &&
	//		(Name.Table.L == "" || strings.EqualFold(Name.Table.L, v.AliasTable)) {
	//		if v.IsMaskField {
	//			return v.MaskFunc, true
	//		}
	//	}
	//}
	//return "", false
}

func PackMaskNode(arg ast.ExprNode, maskFunc string) ast.ExprNode {
	newField := &ast.FuncCallExpr{}
	newField.FnName = model.CIStr{maskFunc, maskFunc}
	newField.Args = append(newField.Args, arg)
	return newField
}

func ProcessArgsMask(args []ast.ExprNode, fieldsRelation *[]*FieldRelation) {
	for i, arg := range args {
		if columnExpr, ok := arg.(*ast.ColumnNameExpr); ok {
			if maskFunc, ok := IsMaskField(columnExpr.Name, fieldsRelation); ok {
				args[i] = PackMaskNode(columnExpr, maskFunc)
			}
		} else {
			args[i] = ProcessFieldMask(arg, fieldsRelation).(ast.ExprNode)
		}
	}
}

// CreateUnshardPlan constructor of UnshardPlan
func CreateUnshardPlan(stmt ast.StmtNode, phyDBs map[string]string, db string, tableNames []*ast.TableName, fields []*FieldRelation) (*UnshardPlan, error) {
	p := &UnshardPlan{
		db:     db,
		phyDBs: phyDBs,
		stmt:   stmt,
	}
	rewriteUnshardTableName(phyDBs, tableNames)
	if st, ok := stmt.(*ast.SelectStmt); ok && len(fields) != 0 {
		ProcessMask(st, &fields)
	}
	rsql, err := generateUnshardingSQL(stmt)
	fmt.Println("==========", rsql)
	if err != nil {
		return nil, fmt.Errorf("generate unshardPlan SQL error: %v", err)
	}
	p.sql = rsql
	return p, nil
}

func rewriteUnshardTableName(phyDBs map[string]string, tableNames []*ast.TableName) {
	for _, tableName := range tableNames {
		if phyDB, ok := phyDBs[tableName.Schema.String()]; ok {
			tableName.Schema.O = phyDB
			tableName.Schema.L = strings.ToLower(phyDB)
		}
	}
}

func generateUnshardingSQL(stmt ast.StmtNode) (string, error) {
	s := &strings.Builder{}
	ctx := format.NewRestoreCtx(format.EscapeRestoreFlags, s)
	_ = stmt.Restore(ctx)
	return s.String(), nil
}

// CreateSelectLastInsertIDPlan constructor of SelectLastInsertIDPlan
func CreateSelectLastInsertIDPlan() *SelectLastInsertIDPlan {
	return &SelectLastInsertIDPlan{}
}

// ExecuteIn implement Plan
func (p *UnshardPlan) ExecuteIn(reqCtx *util.RequestContext, se Executor) (*mysql.Result, error) {
	r, err := se.ExecuteSQL(reqCtx, backend.DefaultSlice, p.db, p.sql)
	if err != nil {
		return nil, err
	}

	// set last insert id to session
	if _, ok := p.stmt.(*ast.InsertStmt); ok {
		if r.InsertID != 0 {
			se.SetLastInsertID(r.InsertID)
		}
	}

	return r, nil
}

// ExecuteIn implement Plan
func (p *SelectLastInsertIDPlan) ExecuteIn(reqCtx *util.RequestContext, se Executor) (*mysql.Result, error) {
	r := createLastInsertIDResult(se.GetLastInsertID())
	return r, nil
}

func createLastInsertIDResult(lastInsertID uint64) *mysql.Result {
	name := "last_insert_id()"
	var column = 1
	var rows [][]string
	var names = []string{
		name,
	}

	var t = fmt.Sprintf("%d", lastInsertID)
	rows = append(rows, []string{t})

	r := new(mysql.Resultset)

	var values = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	r, _ = mysql.BuildResultset(nil, names, values)
	ret := &mysql.Result{
		Resultset: r,
	}

	return ret
}
