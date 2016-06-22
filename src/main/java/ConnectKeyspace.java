import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dongyan on 14/04/16.
 */

public class ConnectKeyspace {
    private static AstyanaxContext<Keyspace> context;
    private static final String HOSTS_DELIMITER = ",";
    protected static int port = 9160;
    protected static String nodeDiscoveryTypeName = "NONE";
    protected static int maxConnectionsPerHost = 10;
    protected static String connectionPoolTypeName = "BAG";

    public static Keyspace initKeyspace(String betaHosts,String betaClustername,String betaReadConsistency,String betaWriteConsistency, String trailblazerKeyspaceName) {
        NodeDiscoveryType nodeDiscoveryType = NodeDiscoveryType.valueOf(nodeDiscoveryTypeName);
        ConnectionPoolType connectionPoolType = ConnectionPoolType.valueOf(connectionPoolTypeName);

        context = new AstyanaxContext.Builder().forCluster(betaClustername).forKeyspace(trailblazerKeyspaceName).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(nodeDiscoveryType)
                        .setConnectionPoolType(connectionPoolType)
                        .setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(betaWriteConsistency))
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(betaReadConsistency)))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("ConnectionPool")
                        .setPort(port).setMaxConnsPerHost(maxConnectionsPerHost).setSeeds(formatSeedHosts(betaHosts)))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();

        Keyspace keyspace = context.getEntity();
        return keyspace;
    }

    private static String formatSeedHosts(String hosts) {
        String[] hostArray = hosts.split(HOSTS_DELIMITER);
        List<String> formatedSeedList = new ArrayList<String>(hostArray.length);

        for (String host : hostArray) {
            formatedSeedList.add(host.trim() + ":" + port);
        }

        return StringUtils.join(formatedSeedList, HOSTS_DELIMITER);
    }

    public static void destroy(){
        context.shutdown();
    }
}