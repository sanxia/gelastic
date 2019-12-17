package gelastic

import (
	"context"
	"errors"
)

import (
	elastic_api "github.com/olivere/elastic"
	"github.com/sanxia/glib"
)

/* ================================================================================
 * Search Impl
 * qq group: 582452342
 * email   : 2091938785@qq.com
 * author  : 美丽的地球啊 - mliu
 * ================================================================================ */

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断索引是否存在
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) IsIndexExists(index ...string) (bool, error) {
	isExists, err := s.client.IndexExists(index...).Do(context.Background())
	if err != nil {
		return false, err
	}

	return isExists, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 判断索引数据是否存在
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) IsDataExists(index, typ, id string) (bool, error) {
	isExists, err := s.client.Exists().Index(index).Type(typ).Id(id).Do(context.Background())
	if err != nil {
		return false, err
	}

	return isExists, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引的文档总数
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetCount(index string) int64 {
	count, _ := s.client.Count(index).Do(context.TODO())
	return count
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引名称集合数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetIndexNames() ([]string, error) {
	return s.client.IndexNames()
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引设置数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetIndexSettings(index ...string) (interface{}, error) {
	if len(index) == 0 {
		return nil, errors.New("argument error")
	}

	return s.client.IndexGetSettings().Index(index...).Do(context.Background())
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引映射数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetIndexMapping(index string) (interface{}, error) {
	if len(index) == 0 {
		return nil, errors.New("argument error")
	}

	return s.client.GetMapping().Index(index).Do(context.Background())
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 设置索引映射数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) SetIndexMapping(index, typ, metaMapping string) (bool, error) {
	if len(index) == 0 || len(metaMapping) == 0 {
		return false, errors.New("argument error")
	}

	if len(typ) == 0 {
		typ = "_doc"
	}

	mappingIndex, err := s.client.PutMapping().Index(index).Type(typ).BodyString(metaMapping).Do(context.Background())
	if err != nil {
		return false, err
	}

	return mappingIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引状态数据
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetIndexStatus(index ...string) (interface{}, error) {
	if len(index) == 0 {
		return nil, errors.New("argument error")
	}

	return s.client.IndexStats().Index(index...).Do(context.Background())
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 创建索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) CreateIndex(index string, metaMapping ...string) (bool, error) {
	if len(index) == 0 {
		return false, errors.New("argument error")
	}

	var metaData string
	if len(metaMapping) > 0 {
		metaData = metaMapping[0]
	}

	createIndex, err := s.client.CreateIndex(index).Body(metaData).Do(context.Background())
	if err != nil {
		return false, err
	}

	return createIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 删除索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) DeleteIndex(index ...string) (bool, error) {
	deleteIndex, err := s.client.DeleteIndex(index...).Do(context.Background())
	if err != nil {
		return false, err
	}

	return deleteIndex.Acknowledged, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 刷新索引
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) RefreshIndex(index ...string) error {
	if len(index) == 0 {
		return errors.New("argument error")
	}

	_, err := s.client.Refresh().Index(index...).Do(context.Background())
	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 保存索引数据到磁盘
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) FlushIndex(index ...string) error {
	if len(index) == 0 {
		return errors.New("argument error")
	}

	_, err := s.client.Flush().Index(index...).Do(context.Background())
	return err
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取索引数据
 * err = json.Unmarshal(*doc.Source, &type)
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetData(index, typ, id string) (interface{}, error) {
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
func (s *search) IndexData(index, typ, id string, data interface{}) error {
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
		if bodyJson, err := glib.ToJson(data); err == nil {
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
 * 分词结果
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) Analyze(index, content string, analyzer ...string) ([]string, error) {
	currentAnalyzer := "ik_max_word"
	tokens := make([]string, 0)

	if len(analyzer) > 0 {
		currentAnalyzer = analyzer[0]
	}

	//分词器
	indexAnalyzer := s.client.IndexAnalyze().Analyzer(currentAnalyzer)

	if len(index) > 0 {
		indexAnalyzer = indexAnalyzer.Index(index)
	}

	//分词结果
	if res, err := indexAnalyzer.Text(content).Do(context.Background()); err != nil {
		return nil, err
	} else {
		for _, token := range res.Tokens {
			tokens = append(tokens, token.Token)
		}
	}

	return tokens, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 查询结果
 * elastic.NewTermQuery("username", "mliu")
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) Query(query elastic_api.Query, option *QueryOption) (*elastic_api.SearchResult, error) {
	//默认查询选项
	if option == nil {
		option = DefaultQueryOption()
	}

	//查询对象
	search := s.client.Search().Query(query).Pretty(true)

	if len(option.Indexs) > 0 {
		search = search.Index(option.Indexs...)
	}

	if len(option.Types) > 0 {
		search = search.Type(option.Types...)
	}

	if option.Size > 0 {
		search = search.From(option.From).Size(option.Size)
	}

	if len(option.SortField) > 0 {
		search = search.Sort(option.SortField, option.IsAscending)
	}

	//获取查询结果
	searchResult, err := search.Do(context.Background())

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
 * 搜索接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) Search() *elastic_api.SearchService {
	return s.client.Search()
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 搜索接口
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) MultiSearch() *elastic_api.MultiSearchService {
	return s.client.MultiSearch()
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * Bulk批处理
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) Bulk(requests ...elastic_api.BulkableRequest) (*elastic_api.BulkResponse, error) {
	bulkRequest := s.client.Bulk()
	return bulkRequest.Add(requests...).Do(context.Background())
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取搜索客户端
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) GetClient() *elastic_api.Client {
	return s.client
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取搜索服务器版本
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (s *search) Version() (string, error) {
	url := "http://127.0.0.1:9200"
	if s.option != nil {
		url = s.option.Hosts[0]
	}

	return s.client.ElasticsearchVersion(url)
}
