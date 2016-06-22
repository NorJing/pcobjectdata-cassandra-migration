import com.datastax.driver.core.Session;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dongyan on 14/04/16.
 */

public class Main {

    private final static Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Session session;

    public static void main(String[] args) {

        long startMigration = DateTimeUtils.currentTimeMillis();
        String betaHosts = "";
        String betaClustername = "TRPC_Beta_Cluster";

        String betaWriteConsistency = "CL_LOCAL_QUORUM";
        String betaReadConsistency = "CL_LOCAL_QUORUM";
        String trailblazerSourceKeyspaceName = "pc_object_data_beta";

        List<String> marketIds = new ArrayList<String>();
        marketIds.add("DEU");
        marketIds.add("ENO");
        List<Long> DEU_Rundates = new ArrayList<Long>();
        List<Long> ENO_Rundates = new ArrayList<Long>();

        try {
            // beta
            Keyspace sourceKeyspace = ConnectKeyspace.initKeyspace(betaHosts, betaClustername, betaReadConsistency, betaWriteConsistency, trailblazerSourceKeyspaceName);
            // prod
            // Keyspace sourceKeyspace = ConnectKeyspace.initKeyspace(dtcProdHosts, prodClustername, prodReadConsistency, prodWriteConsistency, trailblazerSourceKeyspaceName);

            LOG.info("Start connecting Cassandra...");
            LOG.info("source keyspace is {}", sourceKeyspace.describeKeyspace().getName());

            C1GetPCObjectData c1GetPCObjectData = new C1GetPCObjectData();
            int objectId = 3974;
            int objectTypeId = 3;
            String filename = String.valueOf(objectId) + "_" + String.valueOf(objectTypeId) + "_" + "object_model_data.csv";
            // c1GetPCObjectData.fetchObjectModelFromObjectIdAndObjectTypeId(objectId, objectTypeId, sourceKeyspace, filename);
            c1GetPCObjectData.fetchObjectDefinitionFromObjectIdAndObjectTypeId(objectId, objectTypeId, sourceKeyspace, "a");

            // stack_market_rundates
            // Get all rundates for table stack_market_rundates
            // DEU_Rundates = FetchCassandraData.fetchRundate(marketIds.get(0), sourceKeyspace);
            // ENO_Rundates = FetchCassandraData.fetchRundate(marketIds.get(1), sourceKeyspace);
            /*
            int rundates = 0;
            for (Long date : DEU_Rundates) {
                LOG.info("DEU rundates {}", date);
                rundates++;
            }
            for (Long date : ENO_Rundates) {
                LOG.info("ENO rundates {}", date);
                rundates++;
            }
            LOG.info("Total rundates is {}", rundates);
            */
            // Write rundates to CSV files
            // FetchCassandraData.writeStackRangetoFiles(DEU_Rundates, "DEU", stackRangeDataFileName);
            // FetchCassandraData.writeStackRangetoFiles(ENO_Rundates, "ENO", stackRangeDataFileName);

            // stack_data
            // Get all row keys for table stack_data
            // List<String> marketIdList = FetchCassandraData.readStackDataRowkeysFromFileReturnMarketIdList(sourceFilePath);
            // List<Long> runDateList = FetchCassandraData.readStackDataRowkeysFromFileReturnRunDateList(sourceFilePath);

            // Get each column data and write to CSV file
            // FetchCassandraData.printStackDataColumnValueToEachFile(keyspace, marketIdList, runDateList, outputFileNameSuffix);
            // FetchCassandraData.writeStackDataToEachFile(sourceKeyspace, "DEU", DEU_Rundates, outputFileNameSuffix);
            // FetchCassandraData.writeStackDataToEachFile(sourceKeyspace, "ENO", ENO_Rundates, outputFileNameSuffix);

            // C3Session c3Session = new C3Session(targetHosts);
            // session = c3Session.initSession(targetKeyspaceName);
            // LOG.info("C3 Connection hosts {}", session.getState().getConnectedHosts());
            // C3ManipulateStackData c3ManipulateStackData = new C3ManipulateStackData(session);

            // write rundates to CS3
            // c3ManipulateStackData.insertStackRunDates(targetKeyspaceName, ENO_Rundates, "ENO");
            // c3ManipulateStackData.insertStackRunDates(targetKeyspaceName, DEU_Rundates, "DEU");

            // write stack_data to CS3
            // c3ManipulateStackData.insertStackDataToCS3(sourceKeyspace, targetKeyspaceName, "DEU", DEU_Rundates);
            // c3ManipulateStackData.insertStackDataToCS3(sourceKeyspace, targetKeyspaceName, "ENO", ENO_Rundates);

            LOG.info("Migration takes {} minutes", ((DateTimeUtils.currentTimeMillis() - startMigration) / 60000.0));
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        ConnectKeyspace.destroy();
        C3Session.destroy();
    }
}
