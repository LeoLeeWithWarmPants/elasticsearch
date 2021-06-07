package org.leolee.elasticsearch;

import org.apache.http.HttpHost;
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
import org.junit.jupiter.api.Test;
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




}
