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
import utils.driver.EsClient;
import utils.driver.EsConfigConsts;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by machuan on 2016/11/25.
 * 删除ES文档.
 * 传参: index , type , 删除规则(日期字段&值匹配, 则删除)
 */
public class TTLWorker {

    public static Logger LOG = Logger.getLogger(TTLWorker.class);
    public static final String PRIMARK_KEY = "_id";
    public static final int SEARCH_SIZE = 1000;
    public static final int SCROLL_EXPIRE_MILLS = 60 * 1000;
    public static final int DEL_BULK_SIZE = 1000;


    /**
     * @param index
     * @param type
     * @param fieldName
     * @param beforeDay
     * @param retry
     * @return
     */
    public static boolean startWorker(String index, String type, String fieldName, int beforeDay, int retry) {
        Client client = EsClient.getClient(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, false);

        SearchRequestBuilder searchRequestBuilder = buildQueryRange(client, index, type, fieldName, buildDelDay(beforeDay));
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        long totalHits = response.getHits().totalHits();
        if (totalHits < 1) {
            System.out.println("____________");
            LOG.warn("query hit is 0. check the query.");
            return false;
        }
        List<String> allDelIds = scroll(client, response); //carefor OOM
        long totalSuccessDel = delDoc(client, index, type, allDelIds);
        LOG.warn("query total hits:" + totalHits + ", del success total num:" + totalSuccessDel + ", retry:" + retry);

        if (totalHits != totalSuccessDel && retry <= 3) {
            return startWorker(index, type, fieldName, beforeDay, retry++);
        } else if (totalHits == totalSuccessDel) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * record all the query doc id
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
            LOG.info("clear the scroll id:" + scrollId + "," + clearScroll(client, scrollId));
            if (response.getHits() == null)
                break;
        }

        return delDocIds;
    }

    public static boolean clearScroll(Client client, String scrollId) {
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.addScrollId(scrollId);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }

    public static long delDoc(Client client, String index, String type, List<String> ids) {
        boolean delSuccess = false;
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

    public static SearchRequestBuilder buildQueryRange(Client client, String indexName, String typeName, String fieldName, String delDay) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setIndices(indexName).setTypes(typeName);
//                .setQuery(QueryBuilders.termQuery("op_time", delDay))
//                .setScroll(new TimeValue(SCROLL_EXPIRE_MILLS))
//                .setSize(SEARCH_SIZE)
//                .setExplain(true);

// post filter
//        searchRequestBuilder.setIndices(indexName).setTypes(typeName)
//                .setPostFilter(QueryBuilders.termQuery("op_time", delDay))
//                .setScroll(new TimeValue(SCROLL_EXPIRE_MILLS))
//                .setSize(SEARCH_SIZE)
//                .setExplain(true);

//        filter query
//      FilteredQueryBuilder filterQuery = QueryBuilders.filteredQuery(QueryBuilders.termQuery("op_time", delDay),null);
//                searchRequestBuilder.setQuery(filterQuery);

        //bool query repalce filter query
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(fieldName, delDay));
        searchRequestBuilder.setQuery(boolBuilder);

        printQueryJsonInRestFul(searchRequestBuilder);
        return searchRequestBuilder;
    }

    public static String buildDelDay(int beforeDays) {
        String delDay = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - beforeDays);
        delDay = sdf.format(calendar.getTime());
        return delDay;
    }

    //print query json for test
    public static void printQueryJsonInRestFul(SearchRequestBuilder builder) {
        System.out.println("restful json:" + builder.toString());
    }

    //
    public static void main(String[] args) {

        String index = "odp_sales_cate_vender_day_all";
        String type = "odp_sales_cate_vender_day_all";
        String field = "op_time";
        int delDay = 30;
        int retry = 0;
        startWorker(index, type, field, delDay, retry);

    }

}
