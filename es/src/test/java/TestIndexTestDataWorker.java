import junit.framework.TestCase;
import org.elasticsearch.client.Client;
import utils.driver.EsClient;
import utils.driver.EsConfigConsts;
import worker.IndexTestDataWorker;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by machuan on 2016/11/30.
 */
public class TestIndexTestDataWorker extends TestCase {

    public void testBulkIndex(){
        String testIndex = "hotword";
        String testType = "hotword";
        Client client = EsClient.getClient(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, false);
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("categoryId3", 672);
        source.put("categoryId2", 671);
        source.put("categoryId", 670);
        source.put("categoryName3", "笔记本");
        source.put("categoryName2","电脑整机");
        source.put("categoryName","笔记本");
        source.put("opTime", 20161130);
        source.put("platform", 99);
        source.put("categoryLev", 3);
        source.put("convertRate", 0.0);
        source.put("saleIndex", 2);
        source.put("clickIndex", 3);
        source.put("word", "联想笔记本300-15");

        IndexTestDataWorker worker = new IndexTestDataWorker();
        long start = System.currentTimeMillis();
        try {
            worker.bulkIndex(client, testIndex, testType, source, 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("total cost:"+(System.currentTimeMillis() - start));

    }
}
