package priv.cyx.java.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * 索引的简单操作
 */
@SpringBootTest
class IndexOperationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 创建索引
     */
    @Test
    void testCCreatIndex() throws IOException {
        // 1、创建索引请求
        /*
            public CreateIndexRequest(String index)：
                index：索引名称
        */
        CreateIndexRequest request = new CreateIndexRequest("java_api_index");
        // 2、使用客户端对象执行请求，请求后获得响应对象
        /*
        * RestHighLevelClient.indices()：
        *       提供一个可用于访问索引 API 的 IndicesClient。
        *
        * RequestOptions.DEFAULT：
        *       一般使用默认的配置。
        */
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 打印响应信息
        System.out.println(response.index());
    }

    /**
     * 测试获取索引，并判断其是否存在
     */
    @Test
    void testGetIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("java_api_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 测试删除索引
     */
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("java_api_index");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }
}
