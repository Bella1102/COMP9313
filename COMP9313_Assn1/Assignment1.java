package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class solves the problem posted for Assignment1
 */

public class Assignment1 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) 
	    		throws IOException, InterruptedException {
	    	
	    	List<String> list = new ArrayList<String>();
	    	
	    	// get value N for the ngram 
	    	String str = context.getConfiguration().get("N");
	    	Integer N = Integer.valueOf(str);
	    	// get file name
	    	FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	// add all words to list
	    	while (itr.hasMoreTokens()) {
	    		list.add(itr.nextToken());
	    	}
	    	// Get N words
	    	for (int i = 0; i < list.size() - N + 1; i++) {
	    		String ngrams = list.get(i) + " ";
	    		if (N > 1) {
	        		for (int j = i + 1; j < i + N - 1 ; j++) {
	        			ngrams += list.get(j) + " ";
	        		}
	        		ngrams = ngrams + list.get(i + N - 1);
	    		} else if (N == 1) {
	    			ngrams = list.get(i);
	    		}
	    		word.set(ngrams);
	    		Text result = new Text(fileName.toString());	    		
	    		context.write(word, result);
	    	}
	    }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    	
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	    		throws IOException, InterruptedException {
	    	
	    	List<String> list = new ArrayList<String>();
	    	// get the minimum count for a ngram to be included in the output
	    	String str = context.getConfiguration().get("count");
	    	Integer count = Integer.valueOf(str);
	    	
	        int sum = 0;    
	        for (Text val : values) {
		        sum += 1;
		        if (!list.contains(val.toString())) {
	      			list.add(val.toString());
	  	        }
		    }
	        Collections.sort(list);
	        // concatenate sum and filenames as a string
	        if (sum >= count) {
	        	String ss = new String(String.valueOf(sum) + "  ");
	        	for (String s: list) {
	        		ss += s + " ";
	        	}
	            context.write(key, new Text(ss));  
	        }
	    }
    }

    public static void main(String[] args) throws Exception {

	    if (args.length != 4) {
	    	System.out.println("This program should have 4 arguments!");
	    	System.exit(1);
	    }
	    try { 
	    	if (!(Integer.valueOf(args[0]) instanceof Integer && Integer.valueOf(args[1]) instanceof Integer)) {
	    		System.out.println("args[0] and args[1] need to be integer string.");
	    	}
	    } catch (Exception e) { 
	    	System.out.println("args[0] and args[1] need to be integer string.");
	    	e.printStackTrace();
	    	System.exit(1);
	    }
	    if (Integer.valueOf(args[0]) <= 0 || Integer.valueOf(args[1]) < 0) {
	    	System.out.println("args[0] and args[1] need to be positive integer.");
	    	System.exit(1);
	    }
	    
	    Configuration conf = new Configuration();
	    conf.set("N", args[0]);
	    conf.set("count", args[1]);
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Assignment1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(job, new Path(args[2]));
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



