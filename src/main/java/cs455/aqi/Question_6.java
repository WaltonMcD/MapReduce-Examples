public class Question_6 {
    
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

            date.set(joinText + "," + year + ",");

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
}
