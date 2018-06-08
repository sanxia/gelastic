package gelastic

import (
	"context"
	"errors"
	"time"
)

import (
	elastic_api "github.com/olivere/elastic"
	"github.com/sanxia/glib"
)

/* ================================================================================
 * Search Index Impl
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断索引是否存在
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) IsIndexExists(index string) (bool, error) {
	s.ensureElasticClient()

	exists, err := elasticClient.IndexExists(index).Do(context.Background())
	if err != nil {
		return false, err
	}

	return exists, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 创建索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) CreateIndex(index, metaMapping string) (bool, error) {
	s.ensureElasticClient()

	createIndex, err := elasticClient.CreateIndex(index).Body(metaMapping).Do(context.Background())
	if err != nil {
		return false, err
	}

	return createIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 删除索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) DeleteIndex(index string) (bool, error) {
	s.ensureElasticClient()

	deleteIndex, err := elasticClient.DeleteIndex(index).Do(context.Background())
	if err != nil {
		return false, err
	}

	return deleteIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 索引数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) IndexData(index, doc string, id string, data interface{}) error {
	s.ensureElasticClient()

	if len(index) == 0 ||
		len(doc) == 0 ||
		len(id) == 0 {
		return errors.New("argument error")
	}

	//json序列化
	bodyJson, jsonErr := glib.ToJson(data)
	if jsonErr != nil {
		return jsonErr
	}

	//索引数据
	_, err := elasticClient.Index().
		Index(index).
		Type(doc).
		Id(id).
		BodyJson(bodyJson).
		Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * Flush索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) FlushIndex(index string) error {
	s.ensureElasticClient()

	if len(index) == 0 {
		return errors.New("argument error")
	}

	_, err := elasticClient.Flush().Index(index).Do(context.Background())
	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 查询数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Query(
	query elastic_api.Query,
	option *QueryOption) (*elastic_api.SearchResult, error) {

	s.ensureElasticClient()

	//默认查询选项
	if option == nil {
		option = DefaultQueryOption()
	}

	searchResult, err := elasticClient.Search().Index(option.Indexs...).Type(option.Types...).Query(query).Sort(option.SortField, option.IsAscending).From(option.From).Size(option.Size).Pretty(true).Do(context.Background())
	return searchResult, err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 返回原始搜索服务接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Search() *elastic_api.SearchService {
	s.ensureElasticClient()

	searchIndex := elastic_api.NewSearchService(elasticClient)
	return searchIndex
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化ElasticClient
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) ensureElasticClient() {
	if elasticClient == nil {
		s.connectionElastic()
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 连接ElasticClient
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) connectionElastic() {
	if client, err := elastic_api.NewClient(
		elastic_api.SetURL(s.Option.Hosts...),
		elastic_api.SetHealthcheckInterval(time.Duration(s.Option.HealthcheckInterval)*time.Second),
		elastic_api.SetMaxRetries(s.Option.MaxRetries)); err == nil {

		elasticClient = client
	}
}
