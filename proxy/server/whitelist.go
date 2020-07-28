package server

import "github.com/ZzzYtl/MyMask/models"

type WhiteList struct {
	name      string
	whitelist map[string]*models.WhiteListRecord
}

func NewWhiteList(config *models.WhiteList) (*WhiteList, error) {
	whitelist := &WhiteList{name: config.Name}

	for _, v := range config.Records {
		whiteRecord := &models.WhiteListRecord{
			IpList:   v.IpList,
			User:     v.User,
			FromTime: v.FromTime,
			ToTime:   v.ToTime,
			Rules:    v.Rules,
		}
		whitelist.whitelist[whiteRecord.User] = whiteRecord
	}
	return whitelist, nil
}
