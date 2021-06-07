package org.leolee.elasticsearch;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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

}
