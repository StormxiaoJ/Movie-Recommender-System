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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize{
	public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text>{
		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//value = movieA:movieB\t relation
			//outputKey = movieA
			//outputValue = movieB=relation
			String[] movie_relation = value.toString().trim().split("\t");
			String movieA = movie_relation[0].split(":")[0];
			String movieB = movie_relation[0].split(":")[1];
			String relation = movie_relation[1];
			context.write(new Text(movieA), new Text(movieB+"="+relation));
		}
	}

	public static class NormalizeReducer extends Reducer<Text, Text, Text, Text>{
		//reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//inputKey = movieA
			//<movieB=relation...>
			//collect all of the relations
			//sum up relations -> denominator
			//iterate movieBs, relation/denominator = relativeRelation
			//下面写出的时候要写的是矩阵的转置，所以key和value的位置调换成
			//outputKey = movieB
			//outputValue = movieA = relativeRelation
			int denominator = 0;
			//map的工作是做出映射movieB和relation
			Map<String , Integer> movie_relation_map = new HashMap<>();
			for(Text value:values){
				//value = movieB=relation
				String[] movieB_relation = value.toString().trim().split("=");
				String movieB = movieB_relation[0];
				int relation = integer.parseint(movieB_relation[1]);
				movie_relation_map.put(movieB, realtion);
				denominator += relation;

			}
			Iterator iterator = movie_realtion_map.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<String,Integer> entry = (Map.Entry<String, Integer>) iterator.next();
				//通过entry来获得movieB的ID，得到movieB之后，再把relation和denominator相除
				String movieB = entry.getKey();
				double relativeRelation = entry.getValue()/denominator;
				context.write(new Text(movieB), new Text(key + "=" + relativeRelation));
			}

		}
	}
}