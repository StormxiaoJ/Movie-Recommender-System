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

public class CoOccurenceMatrixGenerator{
	public static class MtrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		//map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//我们需要知道每两部电影被多少人看过
			//所以要知道电影两两组合的count
			//value = UserID\t movie1:rating1, movie2:rating2....
			//outputKey = movieA:movieB
			//outputValue = 1

			String line = value.toString().trim();
			String[] user_movieRating = line.split("\t");
			if(user_movieRating.length != 2){
				return;
				//这个用户没有rating history
			}
			String movie_rating = user_movieRating[1].split(",");
			if(movie_rating.length == 0){
				//这个用户没有rating history
				//error handling, dirty data
				return;
			}
			// [movie1:rating1, movie2:rating2......]
			for(int i = 0 ; i < movie_rating.length; i++){
				String movieA = movie_rating[i].split(":")[0];
				for(int j = 0 ; j < movie_rating.length; j++){
					String movieB = movie_ratring[j].split(":")[0];
					context.write(new Text(movieA+":"+movieB), new IntWritable(1));
					//当前这两个电影被一个人看过

				}
			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable value: values){
				sum+=value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}
}