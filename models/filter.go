package models

import "encoding/xml"

type FilterList struct {
	Name    string
	Filters []Filter `xml:"Filter"`
}

type Filter struct {
	Name   string `xml:"name,attr"`
	Action Action `xml:"Action"`
}

type Action struct {
	Mask Mask `xml:"Mask"`
}

//<Mask useTemplate="0" function="MASK_CELLPHONE_NUMBER_OPERATOR" dataType="手机号"
//template="常量替换手机号码前4-7位" column_name=""mobile"" databasename="test"
//table_name=""customer"" schemaName=""test""/>
type Mask struct {
	Function     string `xml:"function,attr"`
	SchemaName   string `xml:"schemaName,attr"`
	DataBaseName string `xml:"databasename,attr"`
	TableName    string `xml:"table_name,attr"`
	ColName      string `xml:"column_name,attr"`
}

// Encode encode json
func (n *FilterList) Encode() []byte {
	bytes, err := xml.Marshal(n.Filters)
	if err != nil {
		return nil
	}
	return bytes
}

// Verify verify namespace contents
func (n *FilterList) Verify() error {
	return nil
}
