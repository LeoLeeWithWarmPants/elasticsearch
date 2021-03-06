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
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
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
     * ????????????: <br>
     * ???es??????????????????
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
     * ????????????: <br>
     * ??????????????????
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

        //get option???s response status
        boolean acknowledged = createIndexResponse.isAcknowledged();
        logger.info("create index: {}", acknowledged);

        //close client
        esCleint.close();
    }

    /**
     * ????????????: <br>
     * ??????????????????
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
            logger.info("{}:{}", entry.getKey(), mappingMetadata.toString());
        }
        logger.info("{}", settings);

        //close client
        esCleint.close();
    }

    /**
     * ????????????: <br>
     * ????????????????????????
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
     * ????????????: <br>
     * ??????????????????
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
     * ????????????: <br>
     * ??????????????????
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
     * ????????????: <br>
     * ??????????????????
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
     * ????????????: <br>
     * ??????????????????
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
     * ????????????: <br>
     * ??????????????????
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
     * ????????????: <br>
     * ????????????????????????
     */
    @Test
    public void batchCreateDocuments() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //??????????????????????????????????????????????????????????????????????????????
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("phone").id("10001").source(XContentType.JSON, "name", "??????10", "brand", "??????", "price", 4000));
        bulkRequest.add(new IndexRequest().index("phone").id("10002").source(XContentType.JSON, "name", "??????mate10", "brand", "??????", "price", 5000));
        bulkRequest.add(new IndexRequest().index("phone").id("10003").source(XContentType.JSON, "name", "??????mate12", "brand", "??????", "price", 6000));
        bulkRequest.add(new IndexRequest().index("phone").id("10004").source(XContentType.JSON, "name", "iphone12", "brand", "apple", "price", 11000));
        bulkRequest.add(new IndexRequest().index("phone").id("10005").source(XContentType.JSON, "name", "iphone11", "brand", "apple", "price", 5500));
        bulkRequest.add(new IndexRequest().index("phone").id("10006").source(XContentType.JSON, "name", "one-plus7", "brand", "??????", "price", 4500));
        BulkResponse bulk = esCleint.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("use time:{}", bulk.getTook());
        logger.info("??????????????????:{}", bulk.getItems());

        //close client
        esCleint.close();
    }

    /**
     * ????????????: <br>
     * ????????????????????????
     */
    @Test
    public void batchDeleteDocument() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //??????????????????????????????????????????????????????????????????????????????
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest().index("phone").id("10001"));
        bulkRequest.add(new DeleteRequest().index("phone").id("10002"));
        BulkResponse bulk = esCleint.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("use time:{}", bulk.getTook());
        logger.info("??????????????????:{}", bulk.getItems());

        //close client
        esCleint.close();
    }


    /**
     * ????????????????????????
     */


    /**
     * ????????????: <br>
     * ????????????????????????????????????
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
        //??????????????????
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
     * ????????????: <br>
     * ????????????????????????????????????
     */
    @Test
    public void conditionQuery() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        //this [name] is the document's name from index [phone], the [???] is the condition keyword
        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "???"));
        searchRequest.source(query);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
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
     * ????????????: <br>
     * ????????????????????????????????????
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
        //??????????????????
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
     * ????????????: <br>
     * ??????????????????
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
        //??????????????????
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
     * ????????????: <br>
     * ????????????????????????
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
        //??????????????????
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
     * ????????????: <br>
     * ???????????????,must,should,???????????????
     */
    @Test
    public void combinedSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //and????????????
        /*boolQueryBuilder.must(QueryBuilders.matchQuery("name", "10"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("brand", "???"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("price", 4500));*/

        //or????????????
        boolQueryBuilder.should(QueryBuilders.matchQuery("name", "10"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("name", "??????"));

        //????????????
//        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(4500));


        queryBuilder.query(boolQueryBuilder);
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
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
     * ????????????: <br>
     * ??????????????????
     */
    @Test
    public void fuzzySearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder();
        //Fuzziness.ONE ?????????????????????term ?????????????????????1????????????????????????
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "???").fuzziness(Fuzziness.ONE);
        queryBuilder.query(fuzzyQueryBuilder);
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
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
     * ????????????: <br>
     * ??????????????????
     */
    @Test
    public void highlineSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "iphone12");
        queryBuilder.query(termQueryBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");

        queryBuilder.highlighter(highlightBuilder);
        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
            Map<String, HighlightField> highlightFields = hitsArray[i].getHighlightFields();
            logger.info("{}:{}", highlightFields.keySet().iterator().next(), highlightFields.get(highlightFields.keySet().iterator().next()));
        }

        //close client
        esCleint.close();
    }


    /**
     * ????????????: <br>
     * ???????????????-??????????????????
     */
    @Test
    public void aggregationMaxSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder();

        //??????price?????????????????????:max price
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("max price").field("price");
        queryBuilder.aggregation(aggregationBuilder);

        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        List<Aggregation> aggregations = searchResponse.getAggregations().asList();
        ParsedMax max = (ParsedMax) searchResponse.getAggregations().asMap().get("max price");
        logger.info("max price:{}", max.getValueAsString());

        //close client
        esCleint.close();
    }


    /**
     * ????????????: <br>
     * ???????????????-???????????????
     */
    @Test
    public void aggregationGroupSearch() throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );

        SearchRequest searchRequest = new SearchRequest().indices("phone");
        SearchSourceBuilder queryBuilder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("price group").field("price");
        queryBuilder.aggregation(aggregationBuilder);

        searchRequest.source(queryBuilder);
        SearchResponse searchResponse = esCleint.search(searchRequest, RequestOptions.DEFAULT);
        //??????????????????
        SearchHits hits = searchResponse.getHits();
        logger.info("total hits num:{}", hits.getTotalHits());
        logger.info("search user time:{}", searchResponse.getTook());
        SearchHit[] hitsArray = hits.getHits();
        for (int i = 0; i < hitsArray.length; i++) {
            logger.info("{}", hitsArray[i].getSourceAsString());
        }

        ParsedLongTerms price_group = (ParsedLongTerms) searchResponse.getAggregations().asMap().get("price group");
        price_group.getBuckets().forEach(e -> logger.info("{}:{}", e.getKey().toString(), e.getDocCount()));

        //close client
        esCleint.close();
    }
}
