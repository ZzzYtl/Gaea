package server

import (
	"fmt"
	"github.com/ZzzYtl/MyMask/models"
	"path"
)

type DataBase struct {
	Ip        string
	Port      int
	Db        string
	User      string
	Pw        string
	WhiteList string
	Rule      string
}

func NewDB(config *models.DataBase) (*DataBase, error) {
	db := &DataBase{
		Ip:   config.IP,
		Port: config.Port,
		Db:   config.MaskDatabaseName,
		User: config.UserName,
		Pw:   config.PW,
	}

	_, file := path.Split(config.WhiteList.File)
	if len(file) == 0 {
		return nil, fmt.Errorf("cant find file in path %s", config.WhiteList.File)
	}
	db.WhiteList = file
	db.Rule = config.Security.Rule
	return db, nil
}
