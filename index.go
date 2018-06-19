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

	exists, err := s.client.IndexExists(index).Do(context.Background())
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

	createIndex, err := s.client.
		CreateIndex(index).
		Body(metaMapping).
		Do(context.Background())
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

	deleteIndex, err := s.client.DeleteIndex(index).Do(context.Background())
	if err != nil {
		return false, err
	}

	return deleteIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断索引数据是否存在
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) IsIndexDataExists(index, typ, id string) (bool, error) {
	s.ensureElasticClient()

	exists, err := s.client.Exists().Index(index).Type(typ).Id(id).Do(context.Background())
	if err != nil {
		return false, err
	}

	return exists, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引数据
 * err = json.Unmarshal(*doc.Source, &type)
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) GetIndexData(index, typ, id string) (interface{}, error) {
	s.ensureElasticClient()

	if len(index) == 0 ||
		len(typ) == 0 ||
		len(id) == 0 {
		return "", errors.New("argument error")
	}

	var source interface{}

	doc, err := s.client.Get().
		Index(index).
		Type(typ).
		Id(id).
		Do(context.Background())

	if err == nil {
		if !doc.Found {
			err = errors.New("not found error")
		} else {
			if doc.Source != nil {
				source = *doc.Source
			}
		}
	}

	return source, err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 索引数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) IndexData(index, typ, id string, data interface{}) error {
	s.ensureElasticClient()

	if len(index) == 0 ||
		len(typ) == 0 ||
		len(id) == 0 {
		return errors.New("argument error")
	}

	var err error
	if bodyString, isOk := data.(string); isOk {
		_, err = s.client.Index().
			Index(index).
			Type(typ).
			Id(id).
			BodyString(bodyString).
			Do(context.Background())
	} else {
		bodyJson, err := glib.ToJson(data)
		if err == nil {
			_, err = s.client.Index().
				Index(index).
				Type(typ).
				Id(id).
				BodyJson(bodyJson).
				Do(context.Background())
		}
	}

	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 刷新内存数据到磁盘
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) FlushIndexData(index string) error {
	s.ensureElasticClient()

	if len(index) == 0 {
		return errors.New("argument error")
	}

	_, err := s.client.Flush().Index(index).Do(context.Background())
	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 简单查询数据
 * elastic.NewTermQuery("username", "mliu")
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Query(
	query elastic_api.Query,
	option *QueryOption) (*elastic_api.SearchResult, error) {

	s.ensureElasticClient()

	//默认查询选项
	if option == nil {
		option = DefaultQueryOption()
	}

	searchResult, err := s.client.Search().
		Index(option.Indexs...).
		Type(option.Types...).
		Query(query).
		Sort(option.SortField, option.IsAscending).
		From(option.From).
		Size(option.Size).
		Pretty(true).
		Do(context.Background())

		/*
		   if searchResult.Hits.TotalHits > 0 {
		       for _, hit := range searchResult.Hits.Hits {
		       	var indexData IndexData
		           err := json.Unmarshal(*hit.Source, &indexData)
		       }
		   }
		*/

	return searchResult, err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 复杂搜索数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Search() *elastic_api.SearchService {
	s.ensureElasticClient()

	searchIndex := elastic_api.NewSearchService(s.client)
	return searchIndex
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * Bulk批处理
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Bulk(requests ...elastic_api.BulkableRequest) (*elastic_api.BulkResponse, error) {
	bulkRequest := s.client.Bulk()
	return bulkRequest.Add(requests...).Do(context.Background())
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取搜索引擎服务版本
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) Version(url string) (string, error) {
	return s.client.ElasticsearchVersion(url)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 初始化ElasticClient
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) ensureElasticClient() {
	if s.client == nil {
		s.initElasticClient()
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * init ElasticClient
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *searchIndex) initElasticClient() {
	if elasticClient, err := elastic_api.NewClient(
		elastic_api.SetURL(s.option.Hosts...),
		elastic_api.SetHealthcheckInterval(time.Duration(s.option.HealthcheckInterval)*time.Second),
		elastic_api.SetMaxRetries(s.option.MaxRetries)); err == nil {

		s.client = elasticClient
	}
}
