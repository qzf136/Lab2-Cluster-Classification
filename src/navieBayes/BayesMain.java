package navieBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import navieBayes.AttributePossiblity.AttrPossibleReduce;
import navieBayes.AttributePossiblity.AttrPosssiblityMap;
import navieBayes.Bayes.BayesMap;
import navieBayes.Bayes.BayesReduce;

public class BayesMain {

	public static void main(String[] args) throws Exception {
		AttributePossiblity.init();
		Configuration conf = new Configuration();
		Job jobAttrP = Job.getInstance(conf, "attribute possiblity");
		jobAttrP.setJarByClass(Bayes.class);
		jobAttrP.setMapperClass(AttrPosssiblityMap.class);
		jobAttrP.setReducerClass(AttrPossibleReduce.class);
		jobAttrP.setMapOutputKeyClass(DoubleWritable.class);
		jobAttrP.setMapOutputValueClass(Text.class);
		jobAttrP.setOutputKeyClass(DoubleWritable.class);
		jobAttrP.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(jobAttrP, new Path("hdfs://localhost:9000/lab2/output/Data/train-r-00000"));
		Path outPath = new Path("hdfs://localhost:9000/lab2/output/Bayes/AttrPoss");
		FileSystem fs = outPath.getFileSystem(conf);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(jobAttrP, outPath);
		for (int i = 9; i <= 18; i++) {
			MultipleOutputs.addNamedOutput(jobAttrP, "falseAttr"+i, TextOutputFormat.class, DoubleWritable.class, DoubleWritable.class);
			MultipleOutputs.addNamedOutput(jobAttrP, "trueAttr"+i, TextOutputFormat.class, DoubleWritable.class, DoubleWritable.class);
		}
		jobAttrP.waitForCompletion(true);
		
		Job jobPredict = Job.getInstance(conf, "predict");
		jobPredict.setJarByClass(Bayes.class);
		jobPredict.setMapperClass(BayesMap.class);
		jobPredict.setReducerClass(BayesReduce.class);
		jobPredict.setMapOutputKeyClass(DoubleWritable.class);
		jobPredict.setMapOutputValueClass(DoubleWritable.class);
		jobPredict.setOutputKeyClass(NullWritable.class);
		jobPredict.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(jobPredict, new Path("hdfs://localhost:9000/lab2/output/Data/test-r-00000"));
        Path outPath2 = new Path("hdfs://localhost:9000/lab2/output/Bayes/predict");
        FileSystem fs2 = outPath2.getFileSystem(conf);
        fs2.delete(outPath2, true);
        FileOutputFormat.setOutputPath(jobPredict, outPath2);
        jobPredict.waitForCompletion(true);
        System.err.println(Bayes.right);
        System.err.println(Bayes.wrong);
        System.err.println("正确率: " + Bayes.right*1.0 / (Bayes.right+Bayes.wrong));
		
	}
	
}
