
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

  @Override
  public  int getPartition(Text key, IntWritable value, int numReducedTask){
	  	if (numReducedTask==0){
	  		return 0;
	  	}
	  	if (key.equals(new Text("CSE"))){
	  		return 0;
	  	}
		if (key.equals(new Text("EEE"))){
	  		return 1;
	  	}
		else {
		System.err.println("Paritioner can only handle either 1 or 2 paritions");
		return 0;
		}
	}
}
