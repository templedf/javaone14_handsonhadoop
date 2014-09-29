package javaone;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HandsOnHadoop extends Configured implements Tool {
  private static final String LOGIN = 
          "(\\d+) in ([A-Z]{2}) at (\\d{2}:\\d{2}:\\d{2}) "
          + "on (\\d{2}/\\d{2}/\\d{2})";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new HandsOnHadoop(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    
    job.setJarByClass(HandsOnHadoop.class);
    
    MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, UsersMap.class);
    MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, LoginsMap.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class UsersMap extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable k = new IntWritable();
    private Text v = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String[] parts = value.toString().split(",");
      
      k.set(Integer.parseInt(parts[0]));
      v.set("-" + parts[5]);
      
      context.write(k, v);
    }
  }
  
  public static class LoginsMap extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final Pattern p = Pattern.compile(LOGIN);
    private IntWritable k = new IntWritable();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      Matcher m = p.matcher(value.toString());
      
      if (m.matches()) {
        k.set(Integer.parseInt(m.group(1)));
        v.set(m.group(2));
        
        context.write(k, v);
      }
    }
  }
  
  public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text v = new Text();
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      Set<String> logins = new HashSet<String>();
      String home = null;
      
      for (Text state: values) {
        if (state.charAt(0) == '-') {
          home = state.toString().substring(1);
        } else {
          logins.add(state.toString());
        }
      }
      
      logins.remove(home);
      v.set(logins.toString());
      
      context.write(key, v);
    }
  }
}
