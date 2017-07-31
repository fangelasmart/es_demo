package client;

import config.EsPopertiesConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by angela on 17/7/7.
 */
public class EsClient {
    private static Settings settings = null;

    static {
        try {
            EsPopertiesConfig conf = EsPopertiesConfig.getInstance();
            settings = ImmutableSettings.settingsBuilder()
                    //指定集群名称
                    .put("cluster.name", conf.getClusterName())
                    .put("client.transport.ping_timeout", "5s")
                    //探测集群中机器状态
                    .put("client.transport.sniff", true).build();


            //System.out.println(conf.getClusterName()+"\t");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @SuppressWarnings("resource")
    public static Client getEsConnection() {
        EsPopertiesConfig conf = EsPopertiesConfig.getInstance();
        return new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(conf.getClusterIpAddress(), conf.getClusterPort()));
    }

}
