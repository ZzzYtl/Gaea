package models

type WhiteList struct {
	Name    string
	Records []WhiteListRecord
}

type WhiteListRecord struct {
	IpList   []string `json:"ipList"`
	User     string   `json:"user"`
	FromTime string   `json:"fromTime"`
	ToTime   string   `json:"toTime"`
	Rules    string   `json:"rules"`
}

// Encode encode json
func (n *WhiteList) Encode() []byte {
	return JSONEncode(n.Records)
}

// Verify verify namespace contents
func (n *WhiteList) Verify() error {
	return nil
}
