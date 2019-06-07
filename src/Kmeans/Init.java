package Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Init {

	public static void InitK(int k) {
		Cluster.K = k;
	}
	
	public static int calculateDistance(List<Integer> p1, List<Integer> p2) {
		int v = 0;
		for (int i = 0; i < p1.size(); i++) {
			int d = p1.get(i) - p2.get(i);
			v = v + d*d;
		}
		return v;
		
	}
	
	public static class InitMap extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			try {
				int index = Integer.parseInt(tokens[0]);
				if (index >=10000 && index < 10100) {
					context.write(new IntWritable(1), value);
				}
			} catch (Exception e) {
//				e.printStackTrace();
			}
		}
	}
	
	public static class InitReduce extends Reducer<IntWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Map<Integer, List<Integer>> pointMap = new HashMap<Integer, List<Integer>>();
			for (Text val : value) {
				String line = val.toString();
				String[] tokens = line.split(",");
				int index = Integer.parseInt(tokens[0]);
				List<Integer> point = new ArrayList<Integer>();
				for (int i = 1; i < tokens.length; i++) {
					point.add(Integer.parseInt(tokens[i]));
				}
				pointMap.put(index, point);
			}
			List<Integer> centerList = new ArrayList<Integer>();
			int[][] distance = new int[100][100];
			for (int i = 10000; i < 10100; i++) {
				for (int j = 10000; j < 10100; j++) {
					distance[i-10000][j-10000] = calculateDistance(pointMap.get(i), pointMap.get(j));
				}
			}
//			int r = new Random().nextInt(100) + 10000;
//			centerList.add(r);
			List<Integer> first = new ArrayList<Integer>();
			for (int i = 0; i < Cluster.LENGTH; i++) {
				List<Integer> list = new ArrayList<Integer>();
				for (int j = 10000; j < 10100; j++) {
					list.add(pointMap.get(j).get(i));
				}
				Integer[] arr = new Integer[100];
				list.toArray(arr);
				Arrays.sort(arr);
				first.add(arr[0]);
			}
			int closest = 1000000000;
			int index1 = 0;
			for (int i = 10000; i < 10100; i++) {
				int dis = calculateDistance(first, pointMap.get(i));
				if (dis < closest) {
					closest = dis;
					index1 = i;
				}
			}
			centerList.add(index1);
			
			int max2 = 0;
			int index2 = 0;
			for (int i = 10000; i < 10100; i++) {
				if (distance[centerList.get(0)-10000][i-10000] > max2) {
					max2 = distance[10000-10000][i-10000];
					index2 = i;
				}
			}
			centerList.add(index2);
			for (int i = 2; i < Cluster.K; i++) {
				int maxMinDistance = 0;
				int maxMinIndex = 0;
				for (int j = 10000; j < 10100; j++) {
					int minDistance = 100000000;
					for (int k = 0; k < i; k++) {
						if (distance[centerList.get(k)-10000][j-10000] < minDistance) {
							minDistance = distance[centerList.get(k)-10000][j-10000];
						}
					}
					if (minDistance > maxMinDistance) {
						maxMinDistance = minDistance;
						maxMinIndex = j;
					}
				}
				centerList.add(maxMinIndex);
			}
			System.err.println(centerList);
			for (int i : centerList) {
				List<Integer> po = pointMap.get(i);
				List<Double> cent = new ArrayList<Double>();
				for (int j = 0; j < po.size(); j++) {
					cent.add((double)po.get(j));
				}
				System.err.println(cent);
				Cluster.center.add(cent);
			}
		}
	}
	
}
