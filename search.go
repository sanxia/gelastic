package gelastic

import (
	"time"
)

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

		IsIndexDataExists(index, typ, id string) (bool, error)
		GetIndexData(index, typ, id string) (interface{}, error)
		IndexData(index, typ, id string, data interface{}) error
		FlushIndexData(index string) error

		Query(query elastic_api.Query, option *QueryOption) (*elastic_api.SearchResult, error)
		Search() *elastic_api.SearchService
		Bulk(requests ...elastic_api.BulkableRequest) (*elastic_api.BulkResponse, error)

		GetTokens(index, text string, analyzer ...string) ([]string, error)

		GetClient() *elastic_api.Client
		Version(url string) (string, error)
	}
)

type (
	searchIndex struct {
		option *IndexOption
		client *elastic_api.Client
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化SearchIndex
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewSearchIndex(option *IndexOption) (ISearchIndex, error) {
	if option == nil {
		option = DefaultIndexOption()
	}

	elasticClient, err := elastic_api.NewClient(
		elastic_api.SetURL(option.Hosts...),
		elastic_api.SetHealthcheckInterval(time.Duration(option.HealthcheckInterval)*time.Second),
		elastic_api.SetMaxRetries(option.MaxRetries))

	if err != nil {
		return nil, err
	}

	return &searchIndex{
		option: option,
		client: elasticClient,
	}, nil
}
