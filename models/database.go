package models

import "encoding/xml"

type DBKey struct {
	Addr string
	Db   string
}

type DataBases struct {
	DBS []DataBase `xml:"Database"`
}

type DataBase struct {
	MaskDatabaseName string     `xml:"mask_database_name,attr"`
	DatabaseName     string     `xml:database_name,attr`
	IP               string     `xml:"address,attr"`
	Port             int        `xml:"port,attr"`
	UserName         string     `xml:"user_name,attr"`
	PW               string     `xml:"password,attr"`
	WhiteList        WhiteListR `xml:"Whitelist"`
	Security         SecurityR  `xml:"Security"`
}

type WhiteListR struct {
	File string `xml:"file,attr"`
}

type SecurityR struct {
	Rule string `xml:"rule,attr"`
}

// Encode encode json
func (n *DataBases) Encode() []byte {
	bytes, err := xml.Marshal(n)
	if err != nil {
		return nil
	}
	return bytes
}

// Verify verify namespace contents
func (n *DataBases) Verify() error {
	return nil
}
