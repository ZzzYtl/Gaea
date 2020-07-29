package models

import (
	"encoding/xml"
	"errors"
)

type RuleList struct {
	Records []RuleListRecord `xml:"Rule"`
}

type RuleListRecord struct {
	ID       int    `xml:"id,attr"`
	Name     string `xml:"name,attr"`
	FileName string `xml:"file_name,attr"`
}

// Encode encode json
func (n *RuleList) Encode() []byte {
	bytes, err := xml.Marshal(n)
	if err != nil {
		return nil
	}
	return bytes
}

// Verify verify namespace contents
func (n *RuleList) Verify() error {
	for _, v := range n.Records {
		if len(v.Name) == 0 || len(v.FileName) == 0 {
			return errors.New("name or file name is nil")
		}
	}
	return nil
}
