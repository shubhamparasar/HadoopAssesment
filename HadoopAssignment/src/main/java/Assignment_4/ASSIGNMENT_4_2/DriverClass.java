package ASSIGNMENT_4_2;

import ASSIGNMENT_4_1.HbaseConnect;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DriverClass {

    public static void main(String[] args) throws Exception {

       // HbaseConnect hbaseConnect = new HbaseConnect();
      //  hbaseConnect.createCafeteriaEmployeeCodeTable();


        List<Scan> scans = new ArrayList<>();
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setAttribute("scan.attributes.table.name", Bytes.toBytes("EMPLOYEE"));
        scans.add(scan);
       Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.setAttribute("scan.attributes.table.name",Bytes.toBytes("BUILDING"));
        scans.add(scan1);

        Configuration configuration = HBaseConfiguration.create();
        System.out.println("configuration "+configuration);
        Job job = Job.getInstance(configuration);
        job.setJobName("MRTableReadWrite");
        job.setJarByClass(DriverClass.class);

        TableMapReduceUtil.initTableMapperJob(scans,MapperClass.class, IntWritable.class, Result.class,job);
        TableMapReduceUtil.initTableReducerJob("EMPLOYEE_CAFETERIA_CODE",ReducerClass.class,job);

        Path output = new Path("hdfs://localhost:9000/assignment4b");

        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/assignment4b"),configuration);
        if(fs.exists(output)){
            fs.delete(output,true);
        }
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/assignment4b"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if (job.isSuccessful()) {
            System.out.println("Cafeteria code added to employee table");
        }




    }

}
