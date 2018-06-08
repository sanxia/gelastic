package gelastic

import (
	elastic_api "github.com/olivere/elastic"
)

/* ================================================================================
 * Search Index
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 搜索索引接口
 * elastic_api.NewMatchAllQuery()
 * elastic_api.NewTermQuery("fieldname", "fieldvalue")
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type (
	ISearchIndex interface {
		IsIndexExists(index string) (bool, error)
		CreateIndex(index, metaMapping string) (bool, error)
		DeleteIndex(index string) (bool, error)

		IndexData(index, doc string, id string, data interface{}) error
		FlushIndex(index string) error

		Query(query elastic_api.Query, option *QueryOption) (*elastic_api.SearchResult, error)
		Search() *elastic_api.SearchService
	}
)

type (
	searchIndex struct {
		Option *IndexOption
	}
)

var (
	elasticClient *elastic_api.Client
)

func NewSearchIndex(option *IndexOption) ISearchIndex {
	if option == nil {
		option = DefaultIndexOption()
	}

	return &searchIndex{
		Option: option,
	}
}
