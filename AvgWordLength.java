import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class AvgWordLength extends Configured implements Tool {

public class LetterMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    String s = value.toString();
    for (String word : s.split("\\W+")) {
      if (word.length() > 0) {
        String letter = word.substring(0, 1).toLowerCase();
        output.collect(new Text(letter), new IntWritable(word.length()));
      }
    }
  }
}

public class AverageReducer extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, DoubleWritable> {

  
  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text, DoubleWritable> output, Reporter reporter)
      throws IOException {

    double sum = 0, count = 0;
    while (values.hasNext()) {
      IntWritable value = values.next();
      sum += value.get();
      count++;
    }
    if (count != 0d) {
      double result = sum / count;
      output.collect(key, new DoubleWritable(result));
    }
  }
}

public class AvgWordLength extends Configured implements Tool {

  
  public int run(String[] args) throws Exception {

    String input, output;
    if(args.length == 2) {
      input = args[0];
      output = args[1];
    } else {
      input = "your-input-dir";
      output = "your-output-dir";
    }

    JobConf conf = new JobConf(getConf(), AvgWordLength.class);
    conf.setJobName(this.getClass().getName());

    FileInputFormat.setInputPaths(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));

    conf.setMapperClass(LetterMapper.class);
    conf.setReducerClass(AverageReducer.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvgWordLength(), args);
    System.exit(exitCode);
  }
}


