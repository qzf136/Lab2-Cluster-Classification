package navieBayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SplitData {

	public static int count = 0;
	
	public static class SplitMap extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(count%5), value);
			count++;
		}
	}
	
	public static class SplitReduce extends Reducer<IntWritable, Text, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> mos;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}		
		
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2) 
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int key = arg0.get();
			if (key >= 0 && key <= 3) {
				for (Text val : arg1) {
					mos.write("train", null, val);
				}
			} else {
				for (Text val : arg1) {
					mos.write("test", null, val);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job jobSplit = Job.getInstance(conf, "split");
		jobSplit.setJarByClass(SplitData.class);
		jobSplit.setMapperClass(SplitMap.class);
		jobSplit.setReducerClass(SplitReduce.class);
		jobSplit.setMapOutputKeyClass(IntWritable.class);
		jobSplit.setMapOutputValueClass(Text.class);
		jobSplit.setOutputKeyClass(NullWritable.class);
		jobSplit.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobSplit, new Path("hdfs://localhost:9000/lab2/input/SUSY.csv"));
        Path outPath = new Path("hdfs://localhost:9000/lab2/output/Data");
        FileSystem fs = outPath.getFileSystem(conf);        
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(jobSplit, outPath);
        MultipleOutputs.addNamedOutput(jobSplit, "train", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(jobSplit, "test", TextOutputFormat.class, NullWritable.class, Text.class);
        jobSplit.waitForCompletion(true);
	}

}
