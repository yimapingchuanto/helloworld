package worker;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by machuan on 2016/11/25.
 * 删除ES文档.
 * 传参: index , type , 删除规则(日期字段&值匹配, 则删除)
 */
public class DeleteIndexDocsWorker {

    public static Logger LOG = Logger.getLogger(DeleteIndexDocsWorker.class);
    public static final String PRIMARK_KEY = "_id";
    public static final int SEARCH_SIZE = 1000;
    public static final int SCROLL_EXPIRE_MILLS = 60 * 1000;
    public static final int DEL_BULK_SIZE = 1000;


    /**
     * @param index
     * @param type
     * @param fieldName
     * @param fieldVal
     * @param retry
     * @return
     */
    public static boolean startWorker(Client client, String index, String type, String fieldName, Object fieldVal, int retry) {
        SearchRequestBuilder searchRequestBuilder = buildQueryRange(client, index, type, fieldName, fieldVal);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        long totalHits = response.getHits().totalHits();
        if (totalHits < 1) {
            System.out.println("____________");
            LOG.warn("query hit is 0. check the query.");
            return false;
        }
        List<String> allDelIds = scroll(client, response); //carefor OOM
        LOG.info("get all doc id in list, list size:" + allDelIds.size() + ",next step is del it");
        long totalSuccessDel = delDoc(client, index, type, allDelIds);
        LOG.warn("query total hits:" + totalHits + ", del success total num:" + totalSuccessDel + ", retry:" + retry);
        if (totalHits != totalSuccessDel && retry > 0) {
            return startWorker(client, index, type, fieldName, fieldVal, retry--);
        } else if (totalHits == totalSuccessDel) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * record all the query doc id
     *
     * @param client
     * @param response
     * @return
     */
    public static List<String> scroll(Client client, SearchResponse response) {
        List<String> delDocIds = new LinkedList<String>();
        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                delDocIds.add(hit.getId());
            }
            String scrollId = response.getScrollId();
            response = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(60 * 1000)).execute().actionGet();
            LOG.info("clear scrollId:"+clearScroll(client, scrollId));
            if (response.getHits() == null || response.getHits().getHits().length < 1)
                break;
        }
        return delDocIds;
    }

    //只有scroll结束的时候,才能clear. 否则,如果当前scroll作为下一次的query参数, 然后清楚当前scrollId则会发送异常
    public static boolean clearScroll(Client client, String scrollId) {
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.addScrollId(scrollId);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }

    public static long delDoc(Client client, String index, String type, List<String> ids) {
        long executeSuccessNum = 0;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int delNum = 0;
        if (ids == null || ids.size() < 1) {
            return executeSuccessNum;
        }
        for (String id : ids) {
            bulkRequest.add(client.prepareDelete().request().index(index).type(type).id(id));
            delNum++;
            if (delNum % DEL_BULK_SIZE == 0) {
                executeSuccessNum = executeSuccessNum + executeBulk(bulkRequest);
                bulkRequest = client.prepareBulk();
            }

        }
        executeSuccessNum = executeSuccessNum + executeBulk(bulkRequest);
        return executeSuccessNum;
    }

    public static int executeBulk(BulkRequestBuilder bulkRequestBuilder) {
        int delSuccessNum = 0;
        if (bulkRequestBuilder.numberOfActions() < 1) {
            return delSuccessNum;
        }
        BulkResponse responses = bulkRequestBuilder.execute().actionGet();
        for (int i = 0; i < responses.getItems().length; i++) {
            if (!responses.getItems()[i].isFailed()) {
                delSuccessNum++;
            }
        }
        return delSuccessNum;
    }

    public static SearchRequestBuilder buildQueryRange(Client client, String indexName, String typeName, String fieldName, Object fieldVal) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setIndices(indexName).setTypes(typeName)
                .setScroll(new TimeValue(SCROLL_EXPIRE_MILLS))
                .setSize(SEARCH_SIZE)
                .setExplain(true);

//      FilteredQueryBuilder filterQuery = QueryBuilders.filteredQuery(QueryBuilders.termQuery("op_time", delDay),null);
//                searchRequestBuilder.setQuery(filterQuery);

        //bool query repalce filter query
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(fieldName, fieldVal));
        searchRequestBuilder.setQuery(boolBuilder);

        printQueryJsonInRestFul(searchRequestBuilder);
        return searchRequestBuilder;
    }

    //print query json for test
    public static void printQueryJsonInRestFul(SearchRequestBuilder builder) {
        System.out.println("restful json:" + builder.toString());
    }

}
