import java.util.*;
import java.io.IOException; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class Cyber
{
	//Mapper class -> output -> string,int
	public static class LogMapper extends MapReduceBase implements 
	Mapper<Object,/* Input Key Type*/
	Text, /* Input Value Type*/
	Text, /* Output Key Type*/
	FloatWritable> /* Output value Type*/
	{
	//Map Function
	public void map(Object key, Text value, OutputCollector<Text, FloatWritable> output,
	Reporter reporter) throws IOException
	{
	String line = value.toString();
	StringTokenizer st=new StringTokenizer(line,"\t");
	int add=0;
	String name=st.nextToken();
	while(st.hasMoreTokens())
	{
	add= add+(Integer.parseInt(st.nextToken()));
	}
	float avgtime=add/7.0f; //calculate average
	output.collect(new Text(name),new FloatWritable(avgtime));
	}
	}
	//Reducer class -. string,int
	public static class LogReducer extends MapReduceBase implements 
	Reducer<Text,FloatWritable,Text,FloatWritable>			      
	{
	//Reduce Function
	public void reduce(Text key,
	Iterator<FloatWritable> values,
	OutputCollector<Text, FloatWritable> output,
	Reporter reporter) 
	throws IOException
	{
	float val=0.0f;
	while(values.hasNext())
	{
		if((val=values.next().get())>5.0f)
			output.collect(key,new FloatWritable(val));
	}
	}
	}
	
	 
	
	public static void main (String args[]) throws Exception
	{
	{
	JobConf conf=new JobConf(Cyber.class);
	conf.setJobName("Internet Log");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);
	conf.setMapperClass(LogMapper.class);
	conf.setReducerClass(LogReducer.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	FileInputFormat.setInputPaths(conf,new Path(args[0]));
	FileOutputFormat.setOutputPath(conf,new Path(args[1]));
	JobClient.runJob(conf);
		
	}
}

		
	
}

	
