import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


    public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String[] stringArr = value.toString().split(",");
            System.out.println(stringArr);
            for (String str : stringArr) {
                word.set(str);
                context.write(word, new IntWritable(1));
            }
        }
    }

