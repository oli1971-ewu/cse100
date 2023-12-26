import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sum_prob_c{

public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
  
	if (args.length != 2) {
      System.out.printf("Usage: Sum <input dir> <output dir>\n");
      System.exit(-1);
    }
	Configuration config =new Configuration();
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    @SuppressWarnings("deprecation")
    Job job = new Job(config,"Sum");
    job.setJarByClass(Sum_prob_c.class);
    
    job.setPartitionerClass(MyPartitioner1.class);
    job.setNumReduceTasks(2);
    
    job.setMapperClass(MapForSum1.class);
    job.setCombinerClass(ReduceForSum1.class);
    job.setReducerClass(ReduceForSum1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("Sum");
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}


