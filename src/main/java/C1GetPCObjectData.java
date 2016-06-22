import com.google.common.collect.ImmutableList;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.*;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import com.netflix.astyanax.model.AbstractComposite.Component;
import java.text.SimpleDateFormat;

/**
 * Created by dongyan on 20/06/16.
 */
public class C1GetPCObjectData {
    private static final Logger log = LoggerFactory.getLogger(C1GetPCObjectData.class);

    private static ColumnFamily<Composite, Integer> CF_OBJECT_MODEL = new ColumnFamily<Composite, Integer>("object_model", CompositeSerializer.get(), IntegerSerializer.get());

    private static ColumnFamily<Composite, Object> CF_OBJECT_DEFINITION = new ColumnFamily<Composite, Object>("object_definition", CompositeSerializer.get(), ObjectSerializer.get());

    private static ColumnFamily<Composite, Long> CF_VERSIONS = new ColumnFamily<Composite, Long>("versions", CompositeSerializer.get(), LongSerializer.get());

    private static ColumnFamily<Composite, Long> CF_LAST_UPDATES = new ColumnFamily<Composite, Long>("last_updates", CompositeSerializer.get(), LongSerializer.get());

    private static ColumnFamily<Composite, Integer> CF_OBJECT_VSC_HASH = new ColumnFamily<Composite, Integer>("vsc_hash", CompositeSerializer.get(), IntegerSerializer.get());

    private static ColumnFamily<Composite, Composite> CF_VSC_DATA = new ColumnFamily<Composite, Composite>("vsc_data", CompositeSerializer.get(), CompositeSerializer.get());

    private static ColumnFamily<Composite, String> CF_FTP_META = new ColumnFamily<Composite, String>("ftp_meta", CompositeSerializer.get(), StringSerializer.get());

    private static final Integer EMPTY_OBJECT = -1;

    //Delimiter used in CSV file
    private static final String PIPE_DELIMITER = "|";
    private static final String NEW_LINE_SEPARATOR = "\n";
    private static final int cassandraTtl = 863995;

    public void fetchAllObjectModel(Keyspace objectDataKeySpace) {
        int fetchSize = 200;
        Rows<Composite, Integer> rows;
        try {
            rows = objectDataKeySpace.prepareQuery(CF_OBJECT_MODEL).getAllRows().execute().getResult();
                   // .withColumnRange(Integer.MIN_VALUE, Integer.MAX_VALUE, false, 1).setIncludeEmptyRows(true)

            // List<Map<String, Integer>> result = new ArrayList<Map<String, Integer>>();
            for (Row<Composite, Integer> row : rows) {
                Composite key = row.getKey();
                // Map<String, Integer> m = new HashMap<String, Integer>();
                int objectId = key.get(0, IntegerSerializer.get());
                int objectTypeId = key.get(1, IntegerSerializer.get());
                String filename = String.valueOf(objectId) + "_" + String.valueOf(objectTypeId) + "_" + "object_model_data.csv";
                this.fetchObjectModelFromObjectIdAndObjectTypeId(objectId,objectTypeId, objectDataKeySpace, filename);
                // m.put("objectId", key.get(0, IntegerSerializer.get()));
                // m.put("objectTypeId", key.get(1, IntegerSerializer.get()));
                // result.add(m);
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    public void fetchObjectModelFromObjectIdAndObjectTypeId (int objectId, int objectTypeId, Keyspace keyspace, String filename) {
        OperationResult<ColumnList<Integer>> columnList = null;
        Composite key = getKey(objectId, objectTypeId);
        try {
            columnList = keyspace.prepareQuery(CF_OBJECT_MODEL).getKey(key).execute();
        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        long modelTimestamp = Long.MIN_VALUE;
        log.info("min timestamp is {}", modelTimestamp);
        List<ValueSetCurveModel> vscs = new ArrayList<ValueSetCurveModel>();

        if (columnList.getResult().isEmpty()) {

        } else {
            try {
                FileWriter fileWriter = new FileWriter("./data/" + filename);
                List<Integer> result = new ArrayList<Integer>();
                for(Column<Integer> column : columnList.getResult()) {
                    int vscId = column.getName();
                    log.info("vscId {}", vscId);

                    // result.add(column.getName());
                    Integer curveId = column.getValue(IntegerSerializer.get());
                    log.info("curve id is {}", curveId);

                    // ValueSetCurveModel model = new ValueSetCurveModel(column.getName());
                    // vscs.add(model);
                    // getting the latest column timestamp to set it as the timestamp of the object model
                    modelTimestamp = column.getTimestamp() > modelTimestamp ? column.getTimestamp() : modelTimestamp;
                    log.info("Timestamp is {}", column.getTimestamp());

                    fileWriter.append(objectId + PIPE_DELIMITER);
                    fileWriter.append(objectTypeId + PIPE_DELIMITER);
                    fileWriter.append(vscId + PIPE_DELIMITER);
                    if (curveId == null) {
                        fileWriter.append(PIPE_DELIMITER);
                    } else {
                        fileWriter.append(curveId + PIPE_DELIMITER);
                    }
                    // Date date = new Date();
                    // fileWriter.append(new SimpleDateFormat("YYYY-MM-dd H:m:s.SSSSSSZ").format(modelTimestamp) + NEW_LINE_SEPARATOR);
                    fileWriter.append(modelTimestamp + NEW_LINE_SEPARATOR);
                }
                fileWriter.flush();
                fileWriter.close();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void fetchObjectDefinitionFromObjectIdAndObjectTypeId (int objectId, int objectTypeId, Keyspace keyspace, String filenameø) {
        Composite key = getKey(objectId, objectTypeId);
        ColumnList<Object> columns;
        try {
            columns = keyspace.prepareQuery(CF_OBJECT_DEFINITION).getKey(key).execute().getResult();
            long timestamp = Long.MIN_VALUE;
            Object definition = null;

            if (columns.size() == 1) {
                Column column = columns.getColumnByIndex(0);
                definition = column.getValue(ObjectSerializer.get());
                if (definition != null) {
                    log.info("defintion is {}", definition);
                }
                timestamp = column.getTimestamp();
                log.info("timstamp is {}", timestamp);
            }else{
                log.info("column size is {}", columns.size());
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }

    private ValueSetCurveModel getValueSetCurveModel(Column<Integer> column) {
        ValueSetCurveModel model = new ValueSetCurveModel(column.getName());
        for (AbstractComposite.Component<?> component : column.getValue(CompositeSerializer.get()).getComponents()) {
            Integer curveId = component.getValue(IntegerSerializer.get());
            model.getCurveIds().add(curveId);
        }
        return model;
    }

    private Composite getKey(int objectId, int objectTypeId) {
        Composite key = new Composite();
        key.addComponent(objectId, IntegerSerializer.get());
        key.addComponent(objectTypeId, IntegerSerializer.get());
        return key;
    }
}
