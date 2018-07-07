import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDividerByUser{
	//统计每个user对所有看过电影的评分
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//input user, movie, rating
			//outputKey = user
			//outputValue = movie:rating
			String[] user_rating_movie = value.toString().trim().split(",");
			context.write(new IntWritable(Integer.pareseInt(user_rating_movie[0]), new Text(user_rating_movie[1]+user_rating_movie[2])));

		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//inputKey = userId
			//inputValue=<movieId:rating.........>
			StringBuilder outputValue = new StringBuilder();
			while(values.iterator().hasNext()){
				outputValue.append(values.iterator().next());
				outputValue.append(",");

			}
			context.write(key, new Text(outputValue.deleteCharAt(outputValue.length()-1).toString()));
		}
	}
}