package ES;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by Administrator on 2017/9/28 0028.
 */
public class Es56 {

    private    TransportClient client ;

    public Es56() throws UnknownHostException {
     }


     /**
      * @param
      * */
     @org.junit.Before
     public   void readFileLine(  ) throws UnknownHostException {
         // client.

         Settings settings = Settings.builder().put("cluster.name", "my-application").build();// 集群名

         client= new PreBuiltTransportClient(settings)
                 .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
         System.out.println("before  ...");

        // client.
         //return null;
     }

     public  void  test() throws IOException {

         XContentBuilder builder = jsonBuilder()
                 .startObject()
                 .field("user", "kimchy")
                 .field("postDate", new Date())
                 .field("message", "trying out Elasticsearch")
                 .endObject();
         String json = builder.string();
     }

      @Test
     public void  getConnet() throws IOException {


          IndexResponse response = client.prepareIndex("twitter", "tweet", "8")
                  .setSource(jsonBuilder()
                          .startObject()
                          .field("user", "kimchy")
                          .field("postDate", new Date())
                          .field("message", "trying out Elasticsearch")
                          .field("name","罗晴明")
                          .endObject()
                  )
                  .get();
         System.out.println("this is conneted   "+ JSON.toJSONString(client));
     }


     @Test
     public  void  bulk() throws IOException {

         BulkRequestBuilder bulkRequest = client.prepareBulk();
         bulkRequest.add(client.prepareIndex("twitter", "tweet", "6")
                 .setSource(jsonBuilder()
                         .startObject()
                         .field("user", "kimchy2")
                         .field("postDate", new Date())
                         .field("message", "another post")
                         .endObject()
                 )
         );
         BulkResponse bulkResponse = bulkRequest.get();
         if (bulkResponse.hasFailures()) {
             System.out.println("connet is error ");
             // process failures by iterating through each bulk response item
         }

         //bulkRequest.
     }

    @Test
    public  void  bulk1() throws IOException {

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "3")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .field("name", "luoqingming")
                        .endObject()
                )
        );
        //bulkRequest.
    }

     /**
      * 查询数据
      * */
    @Test
    public  void  query() throws IOException {

        QueryBuilder qb = termQuery("user", "kimchy");

        //macth  搜索
        QueryBuilder mqb = matchQuery(
                "name",
                "罗晴明"
        );


        SearchResponse scrollResp = client.prepareSearch("twitter")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.DESC)
                .setScroll(new TimeValue(60000))
                .setQuery(mqb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
       //Scroll until no hits are returned
        SearchHit  hit;
        System.out.println(JSON.toJSONString(scrollResp.getHits().getHits() ));
//        do {
//            for (SearchHit hit : scrollResp.getHits().getHits()) {
//                //Handle the hit...
//            }
//
//            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//        } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        //bulkRequest.
    }

     @Test
     public  void  getdata(){

         GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
         System.out.println("this is rends is " + JSON.toJSONString(response ) );

     }


    /**
     * 聚合查找数据
     * */
    private  XContentBuilder createIKMapping(String indexType) {
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory.jsonBuilder().startObject()
                    // 索引库名（类似数据库中的表）
                    .startObject(indexType).startObject("properties")
                    .startObject("product_name").field("type", "string")
                    .field("analyzer","ik").field("search_analyzer","ik_smart").endObject()
                    .startObject("title_sub").field("type", "string")
                    .field("analyzer","ik").field("search_analyzer","ik_smart").endObject()
                    .startObject("title_primary").field("type", "string")
                    .field("analyzer","ik").field("search_analyzer","ik_smart").endObject()
                    .startObject("publisher").field("type", "string")
                    .field("analyzer","ik").field("search_analyzer","ik_smart").endObject()
                    .startObject("author_name").field("type", "string")
                    .field("analyzer","ik").field("search_analyzer","ik_smart").endObject()
                    //.field("boost",100).endObject()
                    // 姓名
                    //.startObject("name").field("type", "string").endObject()
                    // 位置
                    //.startObject("location").field("type", "geo_point").endObject()
                    //.endObject().startObject("_all").field("analyzer","ik").field("search_analyzer","ik").endObject().endObject().endObject();
                    .endObject().endObject().endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }






}
