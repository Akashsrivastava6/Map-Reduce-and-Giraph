import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


enum Usercounter{
	map1,map2,reduce1,reduce2;
}
public class Project3MapReduce {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
  
	   String s=itr.nextToken();
	   if(!s.startsWith("#")){
		   word.set(itr.nextToken());
		   context.getCounter(Usercounter.map1).increment(1);
		   context.write(word, one);
		   
	   
   }
 }
}
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, Text>{
 Scanner sc;
 HashMap<String, String> map=new HashMap<String, String>();
 protected void setup(Context context) throws IOException,	InterruptedException {
			Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path eachPath : stopWordsFiles) {
				readFile(eachPath);
			}}
 private void readFile(Path filePath) {
     try{
         BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
         String line = null;
         while(( line= bufferedReader.readLine()) != null) {
             String lineArray[]=line.split("\t");
             map.put(lineArray[0],lineArray[1]);
         }
     } catch(IOException ex) {
         System.err.println("Exception while reading stop words file: " + ex.getMessage());
     } }
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
   while (itr.hasMoreTokens()) {
	   String mapKey=itr.nextToken();
	  
	   if(!mapKey.startsWith("#")&& itr.hasMoreTokens()){
		   String s=itr.nextToken();
		   context.getCounter(Usercounter.map2).increment(1);
		   context.write(new Text(s),new Text(mapKey+"\t"+map.get(mapKey)));
	   }
   }
 }
}

public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();
 

 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;

   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   context.getCounter(Usercounter.reduce1).increment(1);
   context.write(key, result);
 }
}

public static class IntSumReducer1
    extends Reducer<Text,Text,Text,IntWritable> {
 private IntWritable result = new IntWritable();
 
 
 public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
 
   for (Text val : values) {
	   StringTokenizer st=new StringTokenizer(val.toString(),"\t");
	   st.nextToken();
	   String secondNum=st.nextToken();
	  
	   if(!secondNum.equals("null")){
		   sum+=Integer.parseInt(secondNum)+1;
	   }
	   else{
		   sum+=1;
	   }
   }
   context.write(key, new IntWritable(sum));
   context.getCounter(Usercounter.reduce2).increment(1);
   result.set(sum);
   
 }

}  
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
 
    
    Job job0=Job.getInstance(conf,"second count");
    job0.setJarByClass(Project3MapReduce.class);
    job0.setMapperClass(TokenizerMapper.class);
    job0.setReducerClass(IntSumReducer.class);
    job0.setOutputKeyClass(Text.class);
    job0.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job0,new Path(args[0]));
    FileOutputFormat.setOutputPath(job0, new Path(args[1]));
    job0.waitForCompletion(true);
    
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Project3MapReduce.class);

    DistributedCache.addCacheFile(new Path("/Project3/Output1/part-r-00000").toUri(), job.getConfiguration());
    job.setMapperClass(TokenizerMapper1.class);
    job.setReducerClass(IntSumReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}