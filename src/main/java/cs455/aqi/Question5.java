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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question5 {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Question 5-1");
        job.setJarByClass(Question5.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Question 5-2");
        job1.setJarByClass(Question5.class);
        job1.setMapperClass(FinalMapper.class);
        job1.setReducerClass(FinalReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
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

            date.set(joinText + "," + weekString + "," + year + ",");

            context.write(date, aqi);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Double total = 0.0;
            Integer count = 0;
            for (IntWritable val : values) {
                total += val.get();
                count++;
                
            }
            Double avg = total/count;
            
            context.write(key, new DoubleWritable(avg));
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line, ",");

            String gis = itr.nextToken();
            String week = itr.nextToken();
            String year = itr.nextToken();
            String aqi = itr.nextToken().trim();

            context.write(new Text(gis), new Text(week + "," + year + "," + aqi));
        }
    }

    public static class FinalReducer extends Reducer<Text,Text,Text,Text> {
        private TreeMap<String, String> tmap;
        private ArrayList<Double> sortedList;
        private ArrayList<String> yearAndWeekList;

        public void setup(Context context) throws IOException, InterruptedException {
            tmap = new TreeMap<String, String>();
            sortedList = new ArrayList<Double>();
            yearAndWeekList = new ArrayList<String>();
            
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text val : values){
                String[] line = val.toString().split(",");
                tmap.put((line[1] + "," + line[0]) , line[2]);
            }

            for(Map.Entry<String,String> entry : tmap.entrySet()) {
                Double value = Double.parseDouble(entry.getValue());
                sortedList.add(value);
                yearAndWeekList.add(entry.getKey());
            }

            Double max = 0.0;
            String yearAndWeek = "";
            for(int i = 0; i < sortedList.size()-1; i++){
                Double diff = sortedList.get(i+1) - sortedList.get(i);
                if(diff > max){
                    max = diff;
                    yearAndWeek = yearAndWeekList.get(i) + "," + yearAndWeekList.get(i+1);
                }
            }
            String[] check = yearAndWeek.split(",");
            Integer weekOne = Integer.parseInt(check[1]);
            Integer weekTwo = Integer.parseInt(check[3]);
            if(weekOne+1 != weekTwo){
                return;
            }

            sortedList = new ArrayList<Double>();
            yearAndWeekList = new ArrayList<String>();
            String out = yearAndWeek + " : " +max;
            context.write(key, new Text(out));
        }
    }
}