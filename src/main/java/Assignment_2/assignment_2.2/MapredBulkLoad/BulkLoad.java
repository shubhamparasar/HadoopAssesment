package Assignment2.MapredBulkLoad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class BulkLoad {

        /**
         * doBulkLoad.
         *
         * @param pathToHFile path to hfile
         * @param tableName
         */
        public static void doBulkLoad(String pathToHFile, String tableName) {
            try {
                Configuration configuration = new Configuration();
                configuration.set("mapreduce.child.java.opts", "-Xmx1g");
                HBaseConfiguration.addHbaseResources(configuration);
                LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
                Connection connection = ConnectionFactory.createConnection(configuration);

                Table table = connection.getTable(TableName.valueOf("table1"));
//                HTable hTable = new HTable(configuration, "tableName");
                Admin admin = connection.getAdmin();
                loadFfiles.doBulkLoad(new Path(pathToHFile), admin,table,
                connection.getRegionLocator(TableName.valueOf("people2")));
                System.out.println("Bulk Load Completed..");
                connection.close();
                table.close();
            } catch(Exception exception) {
                exception.printStackTrace();
            }
        }
    }
