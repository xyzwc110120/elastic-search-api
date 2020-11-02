package priv.cyx.java.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import priv.cyx.java.elasticsearch.pojo.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 文档的简单操作
 */
@SpringBootTest
public class DocumentOperationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 添加文档
     */
    @Test
    void testAddDoc() throws IOException {
        User user = new User("张三", 30);
        // 创建请求
        IndexRequest request = new IndexRequest("java_api_index");
        // 设置规则：put /java_api_index/_doc/1
        // 设置文档 id
        request.id("1");
        // 设置请求超时时间，默认为 1m
        request.timeout(TimeValue.timeValueSeconds(2));
        // 也可以这样写
        // request.timeout("2s");

        // 将我们的数据放入请求（需要转换成 json 格式，这里我们使用 SpringMVC 自带的 Jackson）
        ObjectMapper mapper = new ObjectMapper();
        // 将文档元数据设置到索引中
        request.source(mapper.writeValueAsString(user), XContentType.JSON);
        // 客户端发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        System.out.println(response.status() );
    }

    /**
     * 获取文档并判断是否存在
     */
    @Test
    void testHaveDoc() throws IOException {
        // 创建 GET 请求
        GetRequest request = new GetRequest("java_api_index", "1");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档信息
     */
    @Test
    void testGetDoc() throws IOException {
        GetResponse response = client.get(new GetRequest("java_api_index", "1"), RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        System.out.println(response);
    }

    /**
     * 更新文档信息
     */
    @Test
    void testUpdateDoc() throws IOException {
        // 创建更新请求
        UpdateRequest request = new UpdateRequest("java_api_index", "1");
        request.timeout(TimeValue.timeValueSeconds(2));

        User user = new User("龙五", 23);

        JsonMapper mapper = new JsonMapper();
        request.doc(mapper.writeValueAsString(user), XContentType.JSON);

        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 删除文档
     */
    @Test
    void testDeleteDoc() throws IOException {
        // 创建删除请求
        DeleteRequest request = new DeleteRequest("java_api_index", "1");
        request.timeout(TimeValue.timeValueSeconds(2));
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 批量插入数据
     */
    @Test
    void testBulkInsertion() throws IOException {
        // 创建批量操作请求
        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueSeconds(10));

        List<User> list = new ArrayList<>();
        list.add(new User("张大爷", 62));
        list.add(new User("李叔", 43));
        list.add(new User("乌鸦", 31));
        list.add(new User("老刘", 51));
        list.add(new User("小五", 19));
        list.add(new User("马三", 37));

        JsonMapper mapper = new JsonMapper();
        // 批处理请求
        for (int i = 0; i < list.size(); i++) {
            request.add(
                    new IndexRequest("java_api_index")
                            .id((i + 1) + "")
                            .source(mapper.writeValueAsString(list.get(i)), XContentType.JSON));
        }

        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        // 查看是否执行失败，返回 false 代表成功
        System.out.println(responses.hasFailures());
    }

    /**
     * 查询
     */
    @Test
    void testSearch() throws IOException {
        // 创建查询请求
        SearchRequest request = new SearchRequest("java_api_index");
        // 查询构造器（为搜索请求提供条件）
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询条件，我们可以使用 QueryBuilders 工具类来实现
        // 精确查询
        // TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "张");
        // 范围查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gt(30).lt(40);
        // 分页
        sourceBuilder.from(0);
        sourceBuilder.size(3);
        // 排序
        sourceBuilder.sort("age", SortOrder.ASC);
        sourceBuilder.query(rangeQueryBuilder);

        // 获取查询
        SearchResponse response = client.search(request.source(sourceBuilder), RequestOptions.DEFAULT);
        // 迭代获取文档中 hits 属性里面的数据信息
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
