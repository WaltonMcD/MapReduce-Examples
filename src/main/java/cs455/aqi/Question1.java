package cs455.aqi;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question1 {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aqi total");
        job.setJarByClass(Question1.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text date = new Text();
        private IntWritable aqi = new IntWritable();
        private DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd.MM.yyyy");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line, ",");
            String joinText = (itr.nextToken());
            aqi.set(Integer.parseInt(itr.nextToken()));

            Long time_ms = Long.parseLong(itr.nextToken())/1000;
            LocalDateTime epoch = LocalDateTime.ofEpochSecond(time_ms, 0, ZoneOffset.UTC);
            DayOfWeek day = epoch.getDayOfWeek();
            String dayString = day.toString();
            date.set(dayString);

            context.write(date, aqi);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            Double count = 0.0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            
            Double average = sum/count;
            result.set(average);
            context.write(key, result);
        }
    }
}
