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
 * 搜索接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
type (
	ISearch interface {
		IsIndexExists(index ...string) (bool, error)
		IsDataExists(index, typ, id string) (bool, error)

		GetCount(index string) int64

		GetIndexNames() ([]string, error)
		GetIndexSettings(index ...string) (interface{}, error)
		GetIndexMapping(index string) (interface{}, error)
		SetIndexMapping(index, metaMapping string, typ ...string) (bool, error)
		GetIndexStatus(index ...string) (interface{}, error)

		CreateIndex(index string, metaMapping ...string) (bool, error)
		DeleteIndex(index ...string) (bool, error)
		RefreshIndex(index ...string) error
		FlushIndex(index ...string) error

		GetData(index, typ, id string) (interface{}, error)
		IndexData(index, typ, id string, data interface{}) error

		Analyze(content string, analyzer ...string) ([]string, error)
		Query(query elastic_api.Query, option *QueryOption) (*elastic_api.SearchResult, error)
		Search() *elastic_api.SearchService
		MultiSearch() *elastic_api.MultiSearchService
		Bulk(requests ...elastic_api.BulkableRequest) (*elastic_api.BulkResponse, error)

		GetClient() *elastic_api.Client
		Version() (string, error)
	}
)

type (
	search struct {
		option *SearchOption
		client *elastic_api.Client
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化Search
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewSearch(option *SearchOption) (ISearch, error) {
	if option == nil {
		option = DefaultSearchOption()
	}

	options := make([]elastic_api.ClientOptionFunc, 0)
	options = append(options, elastic_api.SetURL(option.Hosts...))
	options = append(options, elastic_api.SetHealthcheckInterval(time.Duration(option.HealthcheckInterval)*time.Second))
	options = append(options, elastic_api.SetMaxRetries(option.MaxRetries))
	options = append(options, elastic_api.SetSniff(option.IsSniff))

	if option.IsAuth {
		options = append(options, elastic_api.SetBasicAuth(option.Username, option.Password))
	}

	elasticClient, err := elastic_api.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return &search{
		option: option,
		client: elasticClient,
	}, nil
}
