package org.leolee.elasticsearch;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.leolee.elasticsearch.test.entity.Phone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class ElasticSearchDemoApplicationTests {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void contextLoads() {
        System.out.println(111);
    }


    /**
     * 功能描述: <br>
     * 〈es客户端创建〉
     */
    @Test
    public void esClientCreateTest() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );
        //close client
        esCleint.close();
    }

    /**
     * 功能描述: <br>
     * 〈创建索引〉
     */
    @Test
    public void createIndex() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * this [phone] is the index's name
         */
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("phone");
        CreateIndexResponse createIndexResponse = esCleint.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        //get option‘s response status
        boolean acknowledged = createIndexResponse.isAcknowledged();
        logger.info("create index: {}", acknowledged);

        //close client
        esCleint.close();
    }

    /**
     * 功能描述: <br>
     * 〈查询索引〉
     */
    @Test
    public void searchIndex() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * this [phone] is the index's name
         */
        GetIndexRequest phone = new GetIndexRequest("phone");
        GetIndexResponse getIndexResponse = esCleint.indices().get(phone, RequestOptions.DEFAULT);
        Map<String, List<AliasMetadata>> aliases = getIndexResponse.getAliases();
        Map<String, MappingMetadata> mappings = getIndexResponse.getMappings();
        Map<String, Settings> settings = getIndexResponse.getSettings();
        logger.info("{}", aliases);
        for (Map.Entry entry : mappings.entrySet()) {
            MappingMetadata mappingMetadata = (MappingMetadata) entry.getValue();
            logger.info("{}:{}", entry.getKey(), mappingMetadata.toString().toString());
        }
        logger.info("{}", settings);

        //close client
        esCleint.close();
    }

    /**
     * 功能描述: <br>
     * 〈查询所有索引〉
     */
    @Test
    public void getAllIndex() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * this [**] is the fuzzy search match keyword, it can be "*e*" or other keywords
         */
        GetIndexRequest allIndexRequest = new GetIndexRequest("**");
        GetIndexResponse getIndexResponse = esCleint.indices().get(allIndexRequest, RequestOptions.DEFAULT);
        String[] indices = getIndexResponse.getIndices();
        for (int i = 0; i < indices.length; i++) {
            logger.info(indices[i]);
        }

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈删除索引〉
     */
    @Test
    public void delIndex() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * this [phone] is the the index's name, null value is delete all index
         */
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("phone");
        AcknowledgedResponse response = esCleint.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        logger.info("delete index:{}", response.isAcknowledged());

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈创建文档〉
     */
    @Test
    public void createDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * build json data for es
         */
        Phone phone = new Phone("iphone12", "apple", 10000);
        Gson gson = new Gson();
        String s = gson.toJson(phone);

        /*
         * this [phone] is the the index's name, [10000] is the specified document id
         */
        IndexRequest indexRequest = new IndexRequest("phone").id("10000").source(s, XContentType.JSON);
        IndexResponse response = esCleint.index(indexRequest, RequestOptions.DEFAULT);
        logger.info("create doc:{}", response.getResult().toString());

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈修改文档〉
     */
    @Test
    public void updateDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        /*
         * this [phone] is the the index's name, [10000] is the specified document id
         */
        UpdateRequest updateRequest = new UpdateRequest().index("phone").id("10000").doc("price", 20000);
        UpdateResponse response = esCleint.update(updateRequest, RequestOptions.DEFAULT);
        logger.info("update doc:{}", response.getResult().toString());
        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈查询文档〉
     */
    @Test
    public void getDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        GetRequest getRequest = new GetRequest().index("phone").id("10000");
        GetResponse documentFields = esCleint.get(getRequest, RequestOptions.DEFAULT);
        logger.info("document info:{}", documentFields.getSourceAsString());

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈删除文档〉
     */
    @Test
    public void delDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        DeleteRequest deleteRequest = new DeleteRequest().index("phone").id("10000");
        DeleteResponse delete = esCleint.delete(deleteRequest, RequestOptions.DEFAULT);
        logger.info("delete document:{}", delete.getResult());

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈批量新增文档〉
     */
    @Test
    public void batchCreateDocuments() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //所谓的批量操作其实就是客户端将单个操作进行了捆绑处理
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("phone").id("10001").source(XContentType.JSON, "name", "小米10", "brand", "小米", "price", 4000));
        bulkRequest.add(new IndexRequest().index("phone").id("10002").source(XContentType.JSON, "name", "华为mate10", "brand", "华为", "price", 5000));
        bulkRequest.add(new IndexRequest().index("phone").id("10003").source(XContentType.JSON, "name", "华为mate12", "brand", "华为", "price", 6000));
        bulkRequest.add(new IndexRequest().index("phone").id("10004").source(XContentType.JSON, "name", "iphone12", "brand", "apple", "price", 11000));
        bulkRequest.add(new IndexRequest().index("phone").id("10005").source(XContentType.JSON, "name", "iphone11", "brand", "apple", "price", 5500));
        bulkRequest.add(new IndexRequest().index("phone").id("10006").source(XContentType.JSON, "name", "one-plus7", "brand", "一加", "price", 4500));
        BulkResponse bulk = esCleint.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("use time:{}", bulk.getTook());
        logger.info("批处理操作项:{}", bulk.getItems());

        //close client
        esCleint.close();
    }

    /**
     * 功能描述: <br>
     * 〈批量删除文档〉
     */
    @Test
    public void batchDeleteDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //所谓的批量操作其实就是客户端将单个操作进行了捆绑处理
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest().index("phone").id("10001"));
        bulkRequest.add(new DeleteRequest().index("phone").id("10002"));
        BulkResponse bulk = esCleint.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("use time:{}", bulk.getTook());
        logger.info("批处理操作项:{}", bulk.getItems());

        //close client
        esCleint.close();
    }


    /**
     * 〈高级查询示例〉
     */


    /**
     * 功能描述: <br>
     * 〈全量查询索引中的数据〉
     */
    @Test
    public void getAllData() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        searchRequest.source(query);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //查询命中数据
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈条件查询索引中的数据〉
     */
    @Test
    public void conditionQuery() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //this [name] is the document's name from index [phone], the [小] is the condition keyword
        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "小"));
        searchRequest.source(query);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //查询命中数据
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈分页查询索引中的数据〉
     */
    @Test
    public void pageQuery() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //this [from(0)] is search starting position, the [size(2)] is the data size of every page
        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).from(0).size(2);
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //查询命中数据
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈排序查询〉
     */
    @Test
    public void sortSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //the [sort("price", SortOrder.DESC)] is mean sort search DESC by [price]
        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort("price", SortOrder.DESC);
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //查询命中数据
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        //close client
        esCleint.close();
    }


    /**
     * 功能描述: <br>
     * 〈指定字段查询〉
     */
    @Test
    public void specifyFieldSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        queryBuilder.fetchSource(new String[]{"name", "price"}, new String[]{"brand"});
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //查询命中数据
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        //close client
        esCleint.close();
    }
}
