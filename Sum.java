import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sum{
	public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		//map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//value = userId:movieId\tsubrating
			String[] key_subRating = value.toString().trim().split("\t");
			context.write(new Text(key_subRating[0]), new DoubleWritable(Double.parseDouble(key_subRating[1])));

		}
	}

	public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			for(DoubleWritable value: values){
				sum+= value.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}
}