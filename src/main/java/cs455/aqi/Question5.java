package cs455.aqi;

import java.io.IOException;

import java.util.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.*;
import java.time.temporal.WeekFields;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question5 {
    
    public static void main(String[] args) throws Exception {
        System.out.println(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Question 5");
        job.setJarByClass(Question5.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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

            WeekFields week = WeekFields.of(Locale.getDefault());
            Integer weekNumber = epoch.get(week.weekOfWeekBasedYear());
            String weekString = weekNumber.toString();

            Integer year = epoch.getYear();

            date.set(joinText + ":" + "Week " + weekString + " of the year " + year);

            context.write(date, aqi);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer max = Integer.MIN_VALUE;
            Integer min = Integer.MAX_VALUE;
            for (IntWritable val : values) {
                Integer value = val.get();
                if(value < min){
                    min = value;
                }
                if(value > max){
                    max = value;
                }
            }
            Integer difference = max - min;
            String res = "|  MAX: " + max + "  MIN: " + min + "  Diff: " + difference;
            
            result.set(res);
            context.write(key, result);
        }
    }
}