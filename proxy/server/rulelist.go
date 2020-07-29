package server

import "github.com/ZzzYtl/MyMask/models"

type RuleList struct {
	name     string
	rulelist map[string]*models.Filter
}

func NewRuleList(config *models.FilterList) (*RuleList, error) {
	rulelist := &RuleList{name: config.Name}
	rulelist.rulelist = make(map[string]*models.Filter, 64)
	for _, v := range config.Filters {
		rulelist.rulelist[v.Name] = &v
	}
	return rulelist, nil
}
