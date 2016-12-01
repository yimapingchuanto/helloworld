import junit.framework.TestCase;
import org.elasticsearch.client.Client;
import utils.driver.EsClient;
import utils.driver.EsConfigConsts;
import worker.DeleteIndexDocsWorker;

/**
 * Created by machuan on 2016/11/30.
 * cl
 */
public class TestTTLWorker extends TestCase {


    public void  testTTL() {
        String testIndex = "hotword";
        String testType = "hotword";
        String field = "opTime";
        String fieldVal = "1975";
        Client client = EsClient.getClient(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, false);
        DeleteIndexDocsWorker.startWorker(client,testIndex, testType, field, fieldVal, 3);


    }
}
