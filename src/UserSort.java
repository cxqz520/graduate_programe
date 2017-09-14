import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.StringSort;

public class UserSort {

    public static class UserSortMap extends Mapper<LongWritable,Text,Text,Text> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //System.out.println(value);
            context.write(new Text("1"),value);
        }
    }

    static class UserSortReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text test:values){
                list.add(test.toString());
            }
            List<StringSort> sorts = new ArrayList<>();
            for (String l: list){
                System.out.println("list1 = "+l.toString().split("    ")[1]);
                sorts.add(new StringSort(l,"    ",1));
            }
            Collections.sort(sorts);
            for(int i = 0;i<100 && i<sorts.size();i++){
                context.write(new Text(),new Text(sorts.get(i).toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "    ");

        Job job = new Job(conf,"UserSort");

        job.setJarByClass(UserSort.class); //设置运行jar中的class名称

        job.setMapperClass(UserSortMap.class);//设置mapreduce中的mapper reducer combiner类
        job.setReducerClass(UserSortReducer.class);
        job.setCombinerClass(UserSortReducer.class);

        job.setOutputKeyClass(Text.class); //设置输出结果键值对类型
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));//设置mapreduce输入输出文件路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);
    }

}