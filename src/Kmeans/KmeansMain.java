package Kmeans;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Kmeans.Cluster.ClusterReduce;
import Kmeans.Init.InitMap;
import Kmeans.Init.InitReduce;


public class KmeansMain {

	public static void main(String[] args) throws Exception {
		Init.InitK(4);
		Configuration conf = new Configuration();
		
		Job jobInit = Job.getInstance(conf, "init");
		jobInit.setJarByClass(Init.class);
		jobInit.setMapperClass(InitMap.class);
		jobInit.setReducerClass(InitReduce.class);
		jobInit.setMapOutputKeyClass(IntWritable.class);
		jobInit.setMapOutputValueClass(Text.class);
		jobInit.setOutputKeyClass(NullWritable.class);
		jobInit.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobInit, new Path("hdfs://localhost:9000/lab2/input/USCensus1990.data.txt"));
        Path outPath = new Path("hdfs://localhost:9000/lab2/output/Kmeans_result");
        FileSystem fs = outPath.getFileSystem(conf);
        fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(jobInit, outPath);
        jobInit.waitForCompletion(true);
        
        int count = 0;
        int hash1 = Cluster.clusterSets.hashCode();
        int hash2 = 0;
        do {
        	hash1 = hash2;
        	Cluster.clusterSets.clear();
			Job jobCLuster = Job.getInstance(conf, "cluster");
			jobCLuster.setJarByClass(Cluster.class);
			jobCLuster.setMapperClass(Cluster.ClusterMap.class);
			jobCLuster.setReducerClass(ClusterReduce.class);
			jobCLuster.setMapOutputKeyClass(IntWritable.class);
			jobCLuster.setMapOutputValueClass(Text.class);
			jobCLuster.setOutputKeyClass(NullWritable.class);
			jobCLuster.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobCLuster, new Path("hdfs://localhost:9000/lab2/input/USCensus1990.data.txt"));
			Path outPath2 = new Path("hdfs://localhost:9000/lab2/output/Kmeans_result");
			FileSystem fs2 = outPath2.getFileSystem(conf);
			fs2.delete(outPath2, true);
			FileOutputFormat.setOutputPath(jobCLuster, outPath2);
			jobCLuster.waitForCompletion(true);
			hash2 = Cluster.clusterSets.hashCode();
			count++;
        }while (hash2 != hash1);
        System.err.println("count = " + count);
        File file = new File("cluster_result.txt");
        if (!file.exists()) {
        	file.createNewFile();
        }
        FileWriter writer = new FileWriter(file);
        StringBuffer buffer = new StringBuffer();
        for (int i = 10000; i < 20000; i++) {
        	buffer.append(Cluster.clusterResult.get(i) + ",");
        }
        writer.write(buffer.toString());
        writer.close();
	}
	
}
