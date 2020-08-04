package server

import (
	"github.com/ZzzYtl/MyMask/models"
	"strings"
	"time"
)

type WhiteList struct {
	name      string
	whitelist map[string]*WhiteListRecord
}

type WhiteListRecord struct {
	IpList   []string
	User     string
	FromTime time.Time
	ToTime   time.Time
	Rules    map[string]bool //set
}

func NewWhiteList(config *models.WhiteList) (*WhiteList, error) {
	whitelist := &WhiteList{name: config.Name}
	whitelist.whitelist = make(map[string]*WhiteListRecord, 64)
	for _, v := range config.Records {
		whiteRecord := &WhiteListRecord{
			IpList: v.IpList,
			User:   v.User,
		}
		var err error
		whiteRecord.FromTime, err = time.ParseInLocation("2006-01-02 15:04:05", v.FromTime, time.Local)
		if err != nil {
			return nil, err
		}
		whiteRecord.ToTime, err = time.ParseInLocation("2006-01-02 15:04:05", v.ToTime, time.Local)
		if err != nil {
			return nil, err
		}
		rules := strings.Split(v.Rules, ";")
		whiteRecord.Rules = make(map[string]bool, 8)
		for _, rule := range rules {
			whiteRecord.Rules[rule] = true
		}
		whitelist.whitelist[whiteRecord.User] = whiteRecord
	}
	return whitelist, nil
}
