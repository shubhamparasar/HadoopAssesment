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

public class HBaseEmployeeBulkLoadDriver {

    private static final String DATA_SEPERATOR = ",";
    private static final String TABLE_NAME = "EMPLOYEE";
    private static final String COLUMN_FAMILY_1 = "employeeDetails";

    public static void main(String[] args) throws Exception {

         //   ASSIGNMENT_4_1.HbaseConnect hbaseConnect = new ASSIGNMENT_4_1.HbaseConnect();
         //   hbaseConnect.createEmployeeTable();

            int result = 1;
            String outputPath = "hdfs://localhost:9000/EmployeeResult";
            Configuration conf = new Configuration();
         /*   conf.set("data.separator", DATA_SEPERATOR);
            conf.set("hbase.table.name", TABLE_NAME);
            conf.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);*/
        conf.set("fs.defaultFS", "hdfs://localhost:9000/EmployeeResult");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            System.out.println("configuration set");
            Job job = Job.getInstance(conf);
            job.setJarByClass(HBaseEmployeeBulkLoadDriver.class);
            job.setJobName("Bulk Loading HBase Table::"+TABLE_NAME);
            job.setMapperClass(TempEmployeeMapper.class);
         //   job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);


            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Employee.csv"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/EmployeeResult"));
            //  FileSystem.getLocal(conf).delete(new Path("hdfs://localhost:9000/"),true);
            //    HFileOutputFormat2.configureIncrementalLoad(job, new HTable(conf,TABLE_NAME));

            TableName tableName = TableName.valueOf("EMPLOYEE");
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

