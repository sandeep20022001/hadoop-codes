import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;


public class Marks
{
	//Mapper class -> output -> string,int
	public static class MarksMapper extends Mapper<Object,Text,Text,IntWritable>
	{
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException
	{
	String line = value.toString();
	StringTokenizer st=new StringTokenizer(line,",");
	String name=st.nextToken();
	int marks= Integer.parseInt(st.nextToken());
	String cls=st.nextToken();
	if(marks>=60)
		context.write(new Text("marks"),new IntWritable(marks));
	}
	}
	//Reducer class -. string,int
	public static class MarksReducer extends Reducer<Text,IntWritable,Text,FloatWritable>
	{
	public void reduce (Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
	{
	FloatWritable avg=new FloatWritable();
	int sum=0;
	int len=0;
	for (IntWritable num : values)
	{
	sum=sum+num.get();
	len +=1;
	}
	avg.set((float)sum/len);
	context.write(new Text("Average is"),avg);
	}
	} 
	
	public static void main (String args[]) throws Exception
	{
	// create the object og Configuration class
	Configuration conf=new Configuration();
	
	// create the object of Job class
	Job job = new Job(conf,"Marks");
	
	// Set the datatype of Output key
	job.setOutputKeyClass(Text.class);
	
	// Set the datatype of Output value
	job.setMapOutputValueClass(IntWritable.class);
	
	// Set the datatype of Output value
	job.setOutputValueClass(FloatWritable.class);
	
	
	// Set the dataformat of Output 
	job.setOutputFormatClass(TextOutputFormat.class);
	
	// Set the dataformat of input 
	job.setInputFormatClass(TextInputFormat.class);
	
	//set the name of mapper class
	job.setMapperClass(MarksMapper.class);
	
	//set the name of reducer class
	job.setReducerClass(MarksReducer.class);
	
	//set the input files path from 0th arguments
	FileInputFormat.addInputPath(job,new Path(args[0]));
	
	//set the input files path from 1st arguments
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	//execute the job and wait for completion
	job.waitForCompletion(true);	
	}
}

	
