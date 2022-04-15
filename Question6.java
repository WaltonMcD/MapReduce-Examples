

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question6 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aqi total");
        job.setJarByClass(Question6.class);
        job.setMapperClass(TimeMapper.class);
        job.setReducerClass(TimeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]+"/stage1"));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "aqi total");
        job2.setJarByClass(Question6.class);
        job2.setMapperClass(StateMapper.class);
        job2.setReducerClass(StateReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]+"/stage1"));
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]+"/stage2"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static class StateMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Text join = new Text(line[0]);
            context.write(join, new Text(line[1]));
        }
    }

    public static class StateReducer extends Reducer<Text,Text,Text,NullWritable> {
        int count = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String state = "";
            String data = "";
            int numi = 0;
            for(Text val : values){
                String v = val.toString();
                String[] items = v.split(" ");

                if(items.length < 2){
                    state = v;
                }else{
                    data = v;
                }
                numi++;
                //data += val.toString();
                //context.write(val, NullWritable.get());
            }
            //context.write(new Text(data), NullWritable.get());

            context.write(new Text(state+","+data+","+ String.valueOf(numi)), NullWritable.get());
                
        }
    }

    public static class TimeMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            //context.write(join,new Text(line[1]));
            Long time_ms = Long.parseLong(line[2])/1000;
            LocalDateTime epoch = LocalDateTime.ofEpochSecond(time_ms, 0, ZoneOffset.UTC);
            Integer year = epoch.getYear();
            context.write(new Text(line[0]+","+ String.valueOf(year)), new Text(line[1]));
        }
    }

    public static class TimeReducer extends Reducer<Text,Text,Text,NullWritable> {
        //int counter = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String out = "";
            int total = 0;
            for (Text val : values) {   
                try{
                    total += Integer.valueOf(val.toString());
                    count++;
                }
                catch (NumberFormatException ex){
                    ex.printStackTrace();
                }
            }
            context.write(new Text(key.toString() + " " + total + "#" + count), NullWritable.get());

            // if(counter < 10){
            //     context.write(new Text(key.toString() + " " + total + "," + count), NullWritable.get());
            //     counter++;
            // }
                
        }
    }
}
