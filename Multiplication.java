import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication{
	public static class CoocurrenceMapper extends Mapper<LongWritable, Text, Text, Text>{
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//value = movieB\tmovieA=relativeRelation
			//outputKey = movieB
			//outputValue = movieA=relativeRelation
			String[] movie_relation = value.toString().trim().split("\t");
			context.write(new Text(movie_relation[0]), new Text(movie_relation[1]));
		}
	}
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text>{
		//map method
		//每一个user都会有自己的ratting matrix
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//value = user, movie, rating
			//outputKey = movie
			//outputValue = user
			String[] user_movie_rating = value.toString().trim().split(",");
			context.write(new Text(user_movie_rating[1]), new Text(user_movie_rating[0]+":"+user_movie_rating[2]));

		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable>{
		@Override
		pulbic void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//inputKey = movieB
			//inputValue = <movieA1=relation, movieA2=relation ... user1:rating1, user2:rating2...>
			//inputValue来自于两个mapper
			//movie_relation_map<movieA_id, relativeRelation> movieA->A1,A2,A3
			//user_rating_map<userId, rating> user ->1,2
			// "A1,1","A1,2".....
			//outputKey = movieA:userId
			//outputValue = relativeRelation*rating

			Map<String, Double> movie_relation_map = new HashMap<String, Double>();
			Map<String, Double> user_rating_map = new HashMap<String,Double>();

			for(Text value:values){
				if(value.toString().contains("=")){
					String[] movie_relation = value.toString().trim().split("=");
					movie_relation_map.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				} else {
					String[] user_rating = value.toString().trim().split(":");
					user_rating_map.put(user_rating[0], Double.parseDouble(user_rating[1]));

				}
				
			}
			for(map.Entry<String,Double> entry: movie_relation_map.entrySet()){
				String movieA = entry.getkey();
				double relation = entry.getValue();
				for(Map.Entry<String,Double> element: user_rating_map.entrySet()){
					String userId = element.getKey();
					double rating = element.getValue();
					String outputKey = userId+":"+movieA;
					double outputValue = relation * rating;
					context.write(new Text(outputKey), new DoubleWritable(outputValue));
					
				}
			}
		}
	}
}