

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.glassfish.jersey.server.Uri;

import java.net.URI;

public class WordCount {

    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://localhost:9000");
        FileSystem hdfs= FileSystem.get(uri,conf);



        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Employee.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
