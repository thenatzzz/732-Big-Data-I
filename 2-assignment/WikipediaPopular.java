// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

        public static class WikipediaPopularMapper
        extends Mapper<LongWritable, Text, Text, LongWritable>{ 
                private LongWritable num_views = new LongWritable(); 
                private Text datetime = new Text();
             
		private final static int INDEX_datetime= 0;
                private final static int INDEX_language= 1;
                private final static int INDEX_pagename = 2;
                private final static int INDEX_views = 3;
	
		@Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
                        
			String[] text_array = value.toString().split(" ");
                        datetime.set(text_array[INDEX_datetime]);
                        num_views.set(Long.parseLong(text_array[INDEX_views]));
			/* NOTE: Do not know why this code does not work!
			if(text_array[INDEX_pagename] =="Main_Page" || text_array[INDEX_pagename].startsWith("Special:") || text_array[INDEX_language] == "en"){
				assert true; // DO NOTHING if meeting above conditions
			}else{
				context.write(datetime,num_views);
			}
			*/
			
			if(!text_array[INDEX_language].equals("en")||text_array[INDEX_pagename].equals("Main_Page")||text_array[INDEX_pagename].startsWith("Special:")){
                                assert true;
                        }else{
                                context.write(datetime,num_views);
                        }

                }
        }
	
  	public static class LongSumReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			int count = 0;
			long max = 0;
			for(LongWritable val: values){   
				if(count ==0){
				   max = val.get();
				}
				if(val.get()>max){
				   max = val.get();
				}
				count = count + 1;
			}
			
			result.set(max);	
			context.write(key, result);
		}
	}                             
        public static void main(String[] args) throws Exception {
                int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args); 
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "wikipedia popular");
                job.setJarByClass(WikipediaPopular.class); 
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(WikipediaPopularMapper.class);
                               
                job.setCombinerClass(LongSumReducer.class);
                job.setReducerClass(LongSumReducer.class);
           
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
	}
}
