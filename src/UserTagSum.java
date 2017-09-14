import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserTagSum {

    public static class WordCountMapper extends Mapper<Object,Text,Text,Text> {

        public static final String DELIMITER = "    ";

        private Text word = new Text();

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] str = value.toString().split(DELIMITER);

            for(int i=1;i<str.length;i++){
                context.write(new Text(str[i]),new Text(str[0]));//同一个标签下就算user的数量
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable totalNum = new IntWritable();

        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            HashMap<String,Integer> has = new HashMap<String ,Integer> ();
            Iterator<Text> it = values.iterator();
            while(it.hasNext()) {
                String str = it.next().toString();
                if(!has.containsKey(str)){
                    has.put(str,1);
                }else {
                    has.put(str,has.get(str)+1);
                }
            }

            Iterator string = has.keySet().iterator();
            while(string.hasNext()){
                String user = (String) string.next();
                ////////////////////////////////////context.write(new Text(user),has.get(user));
            }
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = new Job(conf,"UserTagSum");

        job.setJarByClass(UserTagSum.class); //设置运行jar中的class名称

        job.setMapperClass(WordCountMapper.class);//设置mapreduce中的mapper reducer combiner类
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class); //设置输出结果键值对类型
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));//设置mapreduce输入输出文件路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        conf.set("mapred.textoutputformat.separator", "    ");//自定义key value 分隔符

        System.exit(job.waitForCompletion(true) ? 0:1);
    }

}