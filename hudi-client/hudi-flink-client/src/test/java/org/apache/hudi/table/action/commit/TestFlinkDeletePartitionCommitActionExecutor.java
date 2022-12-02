package org.apache.hudi.table.action.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;


public class TestFlinkDeletePartitionCommitActionExecutor extends HoodieFlinkClientTestHarness {


    @TempDir
    protected java.nio.file.Path tempDir;

    public Configuration hadoopConf(){
        return context.getHadoopConf().get();
    }

    @BeforeEach
    public void setUp() throws Exception{
        initPath();
        initFlinkMiniCluster();
        initTestDataGenerator();
        initFileSystem();
        initMetaClient();
    }


    @Test
    public void testSimpleInsertAndUpdateByCopyAndWrite(HoodieFileFormat fileFormat, boolean populateMetaFile) throws IOException {
        Properties properties = populateMetaFile ? new Properties() : getPropertiesForKeyGen();
        properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.toString());
        HoodieTableMetaClient metaClient = getHoodieMetaClient(properties,HoodieTableType.COPY_ON_WRITE);
    }


    @AfterEach
    public void cleanUp(){

    }

    protected Properties getPropertiesForKeyGen() {
        return getPropertiesForKeyGen(false);
    }

    protected Properties getPropertiesForKeyGen(boolean populateMetaFields) {
        Properties properties = new Properties();
        properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
        properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
        properties.put("hoodie.datasource.write.recordkey.field", "partition_path");
        properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
        properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
        properties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, SimpleAvroKeyGenerator.class);
        return  properties;
    }

    protected HoodieTableMetaClient getHoodieMetaClient(Properties properties, HoodieTableType tableType) throws IOException {
        properties = HoodieTableMetaClient.withPropertyBuilder()
                .setTableName(RAW_TRIPS_TEST_NAME)
                .setTableType(tableType)
                .setPayloadClass(OverwriteWithLatestAvroPayload.class)
                .fromProperties(properties).build();

        return  HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
    }


}
