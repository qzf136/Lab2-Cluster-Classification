package navieBayes;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Bayes {

	public static double PTrue = AttributePossiblity.TrueCount*1.0 / AttributePossiblity.TrueCount + AttributePossiblity.FalseCount;
	public static double PFalse = AttributePossiblity.FalseCount*1.0 / AttributePossiblity.TrueCount + AttributePossiblity.FalseCount;
	
	public static int right = 0;
	public static int wrong = 0;
	
	public static class BayesMap extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] tokens = line.split(",");
			double result = Double.parseDouble(tokens[0]);
			double predict_true = 1;
			for (int i = 0; i < 10; i++) {
				double val_all = Double.parseDouble(tokens[9+i]);
				double val = Double.parseDouble(String.format("%.3f", val_all));
				Double p = AttributePossiblity.trueAttrPossiblity.get(i).get(val);
				if (p == null) {
					p = 1.0 / AttributePossiblity.TrueCount;
				}
				predict_true = predict_true * p;
			}
			predict_true *= PTrue;
			
			double predict_false = 1;
			for (int i = 0; i < 10; i++) {
				double val_all = Double.parseDouble(tokens[9+i]);
				double val = Double.parseDouble(String.format("%.3f", val_all));
				Double p = AttributePossiblity.falseAttrPossiblity.get(i).get(val);
				if (p == null) {
					p = 1.0 / AttributePossiblity.FalseCount;
				}
				predict_false = predict_false * p;
			}
			predict_false *= PFalse;
			if (predict_true > predict_false) {
				context.write(new DoubleWritable(1), new DoubleWritable(result));
			} else {
				context.write(new DoubleWritable(0), new DoubleWritable(result));
			}
		}
	}
	
	public static class BayesReduce extends Reducer<DoubleWritable, DoubleWritable, NullWritable, NullWritable> {
		@Override
		protected void reduce(DoubleWritable arg0, Iterable<DoubleWritable> arg1, Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (DoubleWritable val : arg1) {
				if (val.get() == arg0.get()) {
					right++;
				} else {
					wrong++;
				}
			}
		}
	}
	
}
