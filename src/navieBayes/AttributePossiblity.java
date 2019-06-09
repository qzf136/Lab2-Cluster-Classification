package navieBayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AttributePossiblity {

	public static List<Map<Double, Double>> trueAttrPossiblity = new ArrayList<Map<Double,Double>>();
	public static List<Map<Double, Double>> falseAttrPossiblity = new ArrayList<Map<Double,Double>>();
	public static List<Map<Double, Integer>> trueAttrCount = new ArrayList<>();
	public static List<Map<Double, Integer>> falseAttrCount = new ArrayList<>();
	public static int TrueCount = 0;
	public static int FalseCount = 0;
	
	public static String format = "%."+2+"f";
	
	public static void Attrinit() {
		for (int i = 0; i <= 18; i++) {
			falseAttrCount.add(new HashMap<Double, Integer>());
			falseAttrPossiblity.add(new HashMap<Double, Double>());
		}
		for (int i = 0; i <= 18; i++) {
			trueAttrCount.add(new HashMap<Double, Integer>());
			trueAttrPossiblity.add(new HashMap<Double, Double>());
		}
	}
	
	public static class AttrPosssiblityMap extends Mapper<Object, Text, DoubleWritable, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split(",");
			double result = Double.parseDouble(tokens[0]);
			if (result == 0)	FalseCount++;
			else TrueCount++;
			context.write(new DoubleWritable(result), value);
		}
	}
	
	public static class AttrPossibleReduce extends Reducer<DoubleWritable, Text, DoubleWritable, DoubleWritable> {
		
		private MultipleOutputs<DoubleWritable, DoubleWritable> mos;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<DoubleWritable, DoubleWritable>(context);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double key = arg0.get();
			if (key == 0) {
				for (Text val : arg1) {
					String line = val.toString();
					String[] tokens = line.split(",");
					for (int i = 1; i <= 18; i++) {
						double attrVal_all = Double.parseDouble(tokens[i]);
						Double attrVal = Double.parseDouble(String.format(format, attrVal_all));
						Map<Double, Integer> attrMap = falseAttrCount.get(i);
						if (attrMap.containsKey(attrVal)) {
							int count = attrMap.get(attrVal);
							attrMap.put(attrVal, count+1);
						} else {
							attrMap.put(attrVal, 1);
						}
					}
				}
				for (int i = 1; i <= 18; i++) {
					List<Double> keys = new ArrayList<Double>(falseAttrCount.get(i).keySet());
			        for (double att_val : keys) {
			        	double poss = falseAttrCount.get(i).get(att_val)*1.0/FalseCount;
			        	falseAttrPossiblity.get(i).put(att_val, poss);
			        	mos.write("falseAttr"+i, att_val, poss);
			        }
				}
			} else {
				for (Text val : arg1) {
					String line = val.toString();
					String[] tokens = line.split(",");
					for (int i = 1; i <= 18; i++) {
						double attrVal_all = Double.parseDouble(tokens[i]);
						Double attrVal = Double.parseDouble(String.format(format, attrVal_all));
						Map<Double, Integer> attrMap = trueAttrCount.get(i);
						if (attrMap.containsKey(attrVal)) {
							int count = attrMap.get(attrVal);
							attrMap.put(attrVal, count+1);
						} else {
							attrMap.put(attrVal, 1);
						}
					}
				}
				for (int i = 1; i <= 18; i++) {
					List<Double> keys = new ArrayList<Double>(trueAttrCount.get(i).keySet());
			        for (double att_val : keys) {
			        	double poss = trueAttrCount.get(i).get(att_val)*1.0/TrueCount;
			        	trueAttrPossiblity.get(i).put(att_val, poss);
			        	mos.write("trueAttr"+i, att_val, poss);
			        }
				}
			}
		}
	}
	
}
