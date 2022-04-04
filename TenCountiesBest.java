

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Comparator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TenCountiesBest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aqi total");
        job.setJarByClass(TenCountiesBest.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // job.waitForCompletion(true); +"/tmp/averages"

        // Configuration conf = new Configuration();
        // Job job2 = Job.getInstance(conf, "aqi total");
        // job2.setJarByClass(TenCountiesBest.class);
        // job2.setMapperClass(TokenizerMapper.class);
        // // job.setCombinerClass(IntSumReducer.class);
        // job2.setReducerClass(IntSumReducer.class);
        // job2.setMapOutputKeyClass(Text.class);
        // job2.setMapOutputValueClass(IntWritable.class);
        // job2.setOutputKeyClass(Text.class);
        // job2.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job2, new Path(args[0]));
        // //FileInputFormat.addInputPath(job, new Path(args[1]));
        // FileOutputFormat.setOutputPath(job2, new Path(args[2]+"/tmp/averages"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Text join = new Text(line[0]);
            int aqi = Integer.parseInt(line[1]);
            Long time_ms = Long.parseLong(line[2])/1000;
            LocalDateTime epoch = LocalDateTime.ofEpochSecond(time_ms, 0, ZoneOffset.UTC);
            int year = epoch.getYear();
            if(year == 2020){
                context.write(join, new IntWritable(aqi));
            }
            
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
        
        private ArrayList<String[]> aqi_averages;
        @Override
        protected void setup(Context context) {
            aqi_averages = new ArrayList<String[]>();
        }
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int size = 0;
            for (IntWritable val : values) {
                size += 1;
                sum += val.get();
            }
            String[] info = {key.toString(),String.valueOf(sum/size)};
            aqi_averages.add(info);
            //context.write( key, new Text(String.valueOf(sum/size)) );
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException { 
            Collections.sort(aqi_averages, new Comparator<String[]>() {
                public int compare(String[] avg1, String[] avg2) {
                    if(Integer.parseInt(avg1[1]) < Integer.parseInt(avg2[1])){
                        return -1;
                    }else if(Integer.parseInt(avg1[1]) > Integer.parseInt(avg2[1])){
                        return 1;
                    }else{
                        return 0;
                    }
                    
                }
            });
            for(String[] i : aqi_averages){
                context.write(new Text(i[0]+","+i[1]), NullWritable.get());
            }
        }
    }
}
