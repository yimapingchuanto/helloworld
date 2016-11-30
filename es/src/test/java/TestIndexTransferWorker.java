import junit.framework.TestCase;
import org.elasticsearch.client.Client;
import utils.driver.EsClient;
import utils.driver.EsConfigConsts;
import worker.IndexTransferWorker;

/**
 * Created by machuan on 2016/11/29.
 */
public class TestIndexTransferWorker extends TestCase {


    public void testIndexAndDataTranfer() throws Exception {
        String testIndex = "hotword";
        Client client = EsClient.getClient(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, false);
        IndexTransferWorker worker = new IndexTransferWorker();
        worker.indexTranfer(client, client, testIndex);
    }

    public void testDataTranfer() {
        String originIndex = "hotword";
        String originType = "hotword";
        String targetIndex = "hotword-transfer";
        String targetType = "hotword";
        Client originClient = EsClient.getClient(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, false);
        Client targetClient = originClient;
        IndexTransferWorker worker = new IndexTransferWorker();
        worker.dataTranfer(originClient, targetClient, originIndex, originType, targetIndex, targetType);
    }
}
