package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HBaseBuildingBulkLoadDriver {


    private static final String DATA_SEPERATOR = ",";
    private static final String TABLE_NAME = "BUILDING";
    private static final String COLUMN_FAMILY_1 = "buildingDetails";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

      //  ASSIGNMENT_4_1.HbaseConnect hbaseConnect = new ASSIGNMENT_4_1.HbaseConnect();
     //   hbaseConnect.createBuildingTable();

        int result = 1;
        String outputPath = "hdfs://localhost:9000/BuildingResult";
        Configuration conf = new Configuration();
         /*   conf.set("data.separator", DATA_SEPERATOR);
            conf.set("hbase.table.name", TABLE_NAME);
            conf.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);*/
        conf.set("fs.defaultFS", "hdfs://localhost:9000/result");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());


        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseBuildingBulkLoadDriver.class);
        job.setJobName("Bulk Loading HBase Table::"+TABLE_NAME);
        job.setMapperClass(HBaseBuildingBulkLoadMapper.class);
      //  job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);


        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Building.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/BuildingResult"));

        TableName tableName = TableName.valueOf(TABLE_NAME);
         Connection conn = ConnectionFactory.createConnection(conf);
             Table tablee = conn.getTable(tableName);
             RegionLocator regionLocator = conn.getRegionLocator(tableName);
            HFileOutputFormat2.configureIncrementalLoad(job, tablee, regionLocator);


        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            System.out.println("job successful");
            HBaseBulkLoad.doBulkUpload(outputPath, TABLE_NAME);
            System.out.println("bulk upload done");
        } else {
            result = -1;
        }
    }
}
