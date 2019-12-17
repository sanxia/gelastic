package gelastic

/* ================================================================================
 * Search Option
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 搜索选项数据域结构
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type SearchOption struct {
	Hosts               []string
	Username            string
	Password            string
	HealthcheckInterval int
	MaxRetries          int
	IsAuth              bool
}

func DefaultSearchOption() *SearchOption {
	return &SearchOption{
		Hosts:               []string{"127.0.0.1:9200"},
		HealthcheckInterval: 10,
		MaxRetries:          15,
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 查询选项数据域结构
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type QueryOption struct {
	Indexs      []string `form:"indexs" json:"indexs"`
	Types       []string `form:"types" json:"types"`
	From        int      `form:"from" json:"from"`
	Size        int      `form:"size" json:"size"`
	SortField   string   `form:"sort_field" json:"sort_field"`
	Timeout     string   `form:"timeout" json:"timeout"`
	IsAscending bool     `form:"is_ascending" json:"is_ascending"`
}

func DefaultQueryOption() *QueryOption {
	return &QueryOption{
		Indexs:      make([]string, 0),
		Types:       make([]string, 0),
		From:        1,
		Size:        10,
		SortField:   "",
		Timeout:     "30s",
		IsAscending: false,
	}
}
