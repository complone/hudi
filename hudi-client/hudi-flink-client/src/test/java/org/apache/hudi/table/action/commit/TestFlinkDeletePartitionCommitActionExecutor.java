package org.apache.hudi.table.action.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;


public class TestFlinkDeletePartitionCommitActionExecutor extends HoodieFlinkClientTestHarness {


    @TempDir
    protected java.nio.file.Path tempDir;

    public Configuration hadoopConf(){
        return context.getHadoopConf().get();
    }

    public String basePath(){
        return basePath.toString();
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
        HoodieWriteConfig.Builder cfg = null;
    }


    @AfterEach
    public void cleanUp(){

    }

    protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
        return getConfigBuilder(autoCommit, HoodieIndex.IndexType.BUCKET);
    }

    protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType hoodieIndex){
        // delete operator can execute shuffle

        HoodieWriteConfig.Builder writeConfig = HoodieWriteConfig.newBuilder().withPath(basePath())
                .withSchema(TRIP_EXAMPLE_SCHEMA)
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .withAutoCommit(autoCommit)
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .compactionSmallFileSize(1024 * 1024 * 1024L)
                        // compact after write execute, can exists write latenry
                        .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
                        //FIXME When rewriting data, preserves existing hoodie_commit_time
                        .withPreserveCommitMetadata(false)
                        .build())
                // enable timeline
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
        .withRemoteServerPort(9091)
        .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(hoodieIndex).build());

       //ClusteringConfig RollbakUsingMarkers
        return writeConfig;

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
