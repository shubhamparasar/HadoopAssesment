package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBulkLoad {

    public HBaseBulkLoad(){
        System.out.println("in the bulk uploading class");
    }
    public static void doBulkUpload(String pathToHFile, String tableName) {
        try {
            Configuration configuration = HBaseConfiguration.create();

            configuration.addResource("core-site.xml");
            configuration.addResource("hbase-site.xml");
            configuration.addResource("hdfs-site.xml");

            configuration.set("mapreduce.child.java.opts", "-Xmx1g");
           // HBaseConfiguration.addHbaseResources(configuration);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
          //  HTable hTable = new HTable(configuration, tableName);
           // loadFfiles.doBulkLoad(new Path(pathToHFile), table);
            loadFfiles.doBulkLoad(new Path(pathToHFile), connection.getAdmin(), table, connection.getRegionLocator(TableName.valueOf(tableName)));
            System.out.println("Bulk Load Completed..");

        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
