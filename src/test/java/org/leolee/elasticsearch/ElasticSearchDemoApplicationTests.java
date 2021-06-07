package org.leolee.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

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

}
