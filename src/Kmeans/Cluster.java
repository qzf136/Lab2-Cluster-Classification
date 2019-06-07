package Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Cluster {

	public static int K;
	public static int LENGTH = 68;
	public static LinkedList<List<Double>> center = new LinkedList<>();
	public static Set<Set<Integer>> clusterSets = new HashSet<Set<Integer>>();
	public static Map<Integer, Integer> clusterResult = new HashMap<Integer, Integer>();
	
	public static double calculateDistance(List<Integer> p1, List<Double> cent) {
		double v = 0;
		for (int i = 0; i < LENGTH; i++) {
			double d = p1.get(i) - cent.get(i);
			v = v + d*d;
		}
		return v;
	}

	public static int getClosestCenter(List<Integer> point) {
		List<Double> distance = new ArrayList<Double>();
		for (int i = 0; i < Cluster.K; i++) {
			double dis = calculateDistance(point, Cluster.center.get(i));
			distance.add(dis);
		}
		Double[] temp = new Double[Cluster.K];
		distance.toArray(temp);
		Arrays.sort(temp);
		return distance.indexOf(temp[0]);
	}
	
	public static List<List<Integer>> parseLine(String line) {
		String[] tokens = line.split(",");
		List<List<Integer>> result = new ArrayList<List<Integer>>();
		List<Integer> index = new ArrayList<Integer>();
		List<Integer> point = new ArrayList<Integer>();
		try {
			int id = Integer.parseInt(tokens[0]);
			index.add(id);
			for (int i = 1; i < tokens.length; i++) {
				point.add(Integer.parseInt(tokens[i]));
			}
			result.add(index);
			result.add(point);
			return result;
		} catch (Exception e) {
			return null;
		}
	}
	
	public static class ClusterMap extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			List<List<Integer>> info = parseLine(line);
			if (info != null) {
				List<Integer> point = info.get(1);
				int close = getClosestCenter(point);
				int index = info.get(0).get(0);
				if (index >= 10000 && index < 20000) {
					clusterResult.put(index, close);
				}
				context.write(new IntWritable(close), value);
			}
		}
	}
	
	public static class ClusterReduce extends Reducer<IntWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Set<Integer> cluster = new HashSet<Integer>();
			List<Integer> temp = new ArrayList<Integer>();
			double[] sumArray = new double[LENGTH];
			int n = 0;
			for (Text val : value) {
				String line = val.toString();
				List<List<Integer>> info = parseLine(line);
				temp.add(info.get(0).get(0));
				List<Integer> point = info.get(1);
				for (int i = 0; i < point.size(); i++) {
					sumArray[i] += point.get(i);
				}
				n++;
				cluster.add(info.get(0).get(0));
			}
			clusterSets.add(cluster);
			List<Double> avg = new ArrayList<Double>();
			for (int i = 0 ; i < LENGTH; i++) {
				avg.add(sumArray[i]/n);
			}
			center.poll();
			center.add(avg);
//			System.err.println(avg);
			for (Integer id : temp) {
				context.write(null, new Text(key.toString() + "  " + id));
			}
		}
	}
	
}
