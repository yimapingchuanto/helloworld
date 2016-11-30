package utils.esUtils;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.io.IOException;
import java.util.Map;

/**
 * Created by machuan on 2016/11/29.
 */
public class EsStaticMethod {

    private EsStaticMethod() {

    }


    /**
     * clear scroll id
     * @param client
     * @param scrollId
     * @return
     */
    public static boolean clearScroll(Client client, String scrollId) {
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.addScrollId(scrollId);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }

    /**
     *
     * @param originAdmin
     * @param index
     * @return
     */
    public static boolean isIndexExists(AdminClient originAdmin, String index) {
        boolean exists = false;
        IndicesExistsResponse indicesExistsResponse = originAdmin.indices().exists(new IndicesExistsRequest().indices(new String[]{index})).actionGet();
        exists = indicesExistsResponse.isExists() ? true : false;
        return exists;
    }

    /**
     * @param originAdmin
     * @param index
     * @return
     */
    public static Map<String, String> getIndexSetttingAsMap(AdminClient originAdmin, String index) {
        GetSettingsResponse response = originAdmin.indices().prepareGetSettings(index).get();
        return response.getIndexToSettings().get(index).getAsMap();
    }

    /**
     * @param originAdmin
     * @param index
     * @return
     * @throws IOException
     */
    public static ImmutableOpenMap<String, MappingMetaData> getIndexTypeMappingAsMap(AdminClient originAdmin, String index) throws IOException {
        ClusterState cs = originAdmin.cluster().prepareState().execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index(index);
        ImmutableOpenMap<String, MappingMetaData> typesMapping = imd.getMappings();
        return typesMapping;
    }
}
