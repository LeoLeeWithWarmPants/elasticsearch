package org.leolee.elasticsearch.test;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @ClassName ESTest
 * @Description: TODO
 * @Author LeoLee
 * @Date 2021/6/7
 * @Version V1.0
 **/
public class ESTest {

    public static void main(String[] args) throws IOException {
        //create client connection object
        RestHighLevelClient esCleint = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1", 9200, "HTTP"))
        );
        //close client
        esCleint.close();
    }
}
