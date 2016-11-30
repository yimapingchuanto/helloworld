package utils.driver;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * Created by machuan on 2016/10/12.
 */
public class EsClient {

    private static TransportClient client;

    public static void init() {
        init(EsConfigConsts.CLUSTER_NAME, EsConfigConsts.CLUSTER_IPS, EsConfigConsts.SNIFF_ON);
    }

    public static void init(String clusterName, String[] hosts, boolean sniffOn) {
        Settings settings = Settings.settingsBuilder()
                .put("client.transport.sniff", sniffOn)
                .put("cluster.name", clusterName).build();
        client = TransportClient.builder().settings(settings).build();
        String[] ips = hosts;
        for (String ip : ips) {
            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip.split(":")[0]), Integer.parseInt(ip.split(":")[1])));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    public static TransportClient getClient(String clusterName, String[] hosts, boolean sniffOn) {
        if (client == null && clusterName == null) {
            init();
        } else if (client == null && clusterName != null && hosts != null){
            init(clusterName, hosts, sniffOn);
        }
        return client;
    }
}
