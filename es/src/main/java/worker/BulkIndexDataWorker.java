package worker;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import utils.esUtils.EsStaticMethod;

import java.util.Map;

/**
 * Created by machuan on 2016/11/30.
 * 批量插入ES。
 */
public class BulkIndexDataWorker {
    public static Logger LOG = Logger.getLogger(BulkIndexDataWorker.class);


    //if id not exists, auto generate id
    public void bulkIndex(Client client, String index, String type, Map<String, Object> source, long repeatNum) throws Exception {
        if (repeatNum <= 0) {
            throw new Exception("repeat num must > 0");
        }
        if (!EsStaticMethod.isIndexExists(client.admin(), index)) {
            throw new Exception("index not exists");
        }
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        long failNum = 0;
        for (long i = 0; i < repeatNum; i++) {
            bulkRequestBuilder.add(new IndexRequest().index(index).type(type).source(source));
            if (i % 1000 == 0) {
                failNum += bulkExecuteFailNum(bulkRequestBuilder);
                bulkRequestBuilder = client.prepareBulk();
            }
        }
        if (bulkRequestBuilder.numberOfActions() > 0) {
            failNum += bulkExecuteFailNum(bulkRequestBuilder);
        }
        LOG.info("need insert num "+ repeatNum+",fail num:"+failNum);
    }

    public int bulkExecuteFailNum(BulkRequestBuilder bulkRequestBuilder) {
        int failNum = 0;
        try {
            BulkResponse bulkResponses = bulkRequestBuilder.execute().actionGet();
            for (BulkItemResponse itemResponse : bulkResponses.getItems()) {
                if (itemResponse.isFailed()) {
                    failNum++;
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            LOG.error("bulk insert es error."+e.getMessage());
        }
        return failNum;
    }
}
