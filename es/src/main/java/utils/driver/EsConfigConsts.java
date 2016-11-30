package utils.driver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by machuan on 2016/10/12.
 */
public class EsConfigConsts {

    public static final String CLUSTER_NAME;
    public static final String[] CLUSTER_IPS;
    public static final boolean SNIFF_ON = false;

    static {
        Properties prop = new Properties();
        InputStream in = Object.class.getResourceAsStream("/es.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        CLUSTER_NAME = prop.getProperty("cluster_name");
        String ipList = prop.getProperty("cluster_ips");
        CLUSTER_IPS = ipList.split(",");
    }
}
