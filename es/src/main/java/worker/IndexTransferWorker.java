package worker;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import utils.esUtils.EsStaticMethod;

import java.util.Map;

/**
 * Created by machuan on 2016/11/29.
 * 索引迁移worker
 */
public class IndexTransferWorker {

    public static Logger LOG = Logger.getLogger(IndexTransferWorker.class);

    /**
     * 在索引之间迁移数据.要求索引的mapping中properties一致即可
     *
     * @param originClient
     * @param targetClient
     * @param originIndex
     * @param originType
     * @param targetIndex
     * @param targetType
     */
    public void dataTranfer(Client originClient, Client targetClient, String originIndex, String originType, String targetIndex, String targetType) {
        SearchRequestBuilder searchRequestBuilder = originClient.prepareSearch();
        searchRequestBuilder.setIndices(originIndex).setTypes(originType).setSize(1000)
                .setScroll(new TimeValue(60 * 1000))
        .addSort("_doc", SortOrder.DESC)
                ;
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = searchRequestBuilder.setQuery(matchAllQueryBuilder).execute().actionGet();
        scroll(originClient, targetClient, response, targetIndex, targetType);
    }

    /**
     *
     * @param originClient
     * @param targetClient
     * @param response
     * @param targetIndex
     * @param targetType
     */
    public void scroll(Client originClient, Client targetClient, SearchResponse response, String targetIndex, String targetType) {
        long startTime = System.currentTimeMillis();
        long totalQueryNum = response.getHits().getTotalHits();
        long totalTransferNum = 0;
        BulkRequestBuilder bulkRequestBuilder = null;
        while (true) {
            int thisQueryNum = 0;
            bulkRequestBuilder = targetClient.prepareBulk();
            for (SearchHit hit : response.getHits().getHits()) {
                thisQueryNum++;
                bulkRequestBuilder.add(new IndexRequest().index(targetIndex).type(targetType).id(hit.getId()).source(hit.getSource()));
            }
            String scrollId = response.getScrollId();
            if (thisQueryNum < 1) {
                break;
            }
            totalTransferNum += bulkExecuteSuccessNum(bulkRequestBuilder);
            LOG.info("this query hit:"+response.getHits().getHits().length+ ", up to now totalTransferNum "+totalTransferNum+
                    ",  takes proportion "+(double)totalTransferNum/(double)totalQueryNum);
            response = originClient.prepareSearchScroll(scrollId).setScroll(new TimeValue(60 * 1000)).execute().actionGet();
            if (response.getHits() == null || response.getHits().getHits().length == 0)
                break;
        }
        LOG.info("need to transfer docs:" + totalQueryNum + ", fact transfer docs:" + totalTransferNum+", total cost:"+(System.currentTimeMillis() - startTime));
    }

    public int bulkExecuteSuccessNum(BulkRequestBuilder bulkRequestBuilder) {
        int successNum = 0;
        try {
            BulkResponse bulkResponses = bulkRequestBuilder.execute().actionGet();
            for (BulkItemResponse itemResponse : bulkResponses.getItems()) {
                if (!itemResponse.isFailed()) {
                    successNum++;
                } else {
                    LOG.error("bulk item execute error:"+itemResponse.getFailureMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("bulk insert exception:" + e.getMessage());
        }
        return successNum;
    }

    /**
     * 索引和mapping全部迁移
     *
     * @param originClient
     * @param targetClient
     * @param index
     */
    public void indexTranfer(Client originClient, Client targetClient, String index) throws Exception {
        AdminClient adminClient = originClient.admin();
        AdminClient targetAdmin = targetClient.admin();
        //origin es index settting
        Map<String, String> settingMap = EsStaticMethod.getIndexSetttingAsMap(adminClient, index);
        //origin es type mapping
        ImmutableOpenMap<String, MappingMetaData> typesMapping = EsStaticMethod.getIndexTypeMappingAsMap(adminClient, index);
        //create index in traget es.
        String createIndex = index + "-transfer";
        for (Object object : typesMapping.keys().toArray()) {
            if (!EsStaticMethod.isIndexExists(targetAdmin, createIndex)) {
                CreateIndexResponse createIndexResponse = targetAdmin.indices().create(new CreateIndexRequest().index(createIndex)
                        .mapping(object.toString(), typesMapping.get(object.toString()).getSourceAsMap())
                        .settings(settingMap))
                        .actionGet();
                System.out.println("create index "+createIndex+",type "+object.toString()+ " success");
            } else {
                throw new Exception("index have exists in target es");
            }
        }
    }


}



