package models

import "encoding/xml"

type DBKey struct {
	Ip   string
	Port int
	Db   string
}

type DataBases struct {
	DBS []DataBase `xml:"Database"`
}

type DataBase struct {
	MaskDatabaseName string     `xml:"mask_database_name"`
	DatabaseName     string     `xml:database_name`
	IP               string     `xml:"address"`
	Port             int        `xml:"port"`
	UserName         string     `xml:"user_name"`
	PW               string     `xml:"password"`
	WhiteList        WhiteListR `xml:"Whitelist"`
	Security         SecurityR  `xml:"Security"`
}

type WhiteListR struct {
	File string `xml:"file, attr"`
}

type SecurityR struct {
	Rule string `xml:"file, attr"`
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
