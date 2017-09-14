import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserTag {


    public static final String DELIMITER = "    ";
    public static final String SONGSSDIR = "listener.txt";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//多路径判断
        if (args.length < 2) {
            System.out.println("参数数量不对，至少两个以上参数：<数据文件输出路径>、<输入路径...>");
            System.exit(1);
        }
//输出结果路径
        String dataOutput = args[0];
//多个路输入径
        String[] inputs = new String[args.length - 1];
        System.arraycopy(args, 1, inputs, 0, inputs.length);

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "    ");//自定义key value 分隔符
        Job job = Job.getInstance(conf, "UserTag");
        job.setJarByClass(UserTag.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//将输出路径和输入路径放入Path中
        Path[] inputPathes = new Path[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            inputPathes[i] = new Path(inputs[i]);
        }
        Path outputPath = new Path(dataOutput);
        FileInputFormat.setInputPaths(job, inputPathes);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String inputPath;
        private String fileCode = "";

        protected void setup(Context context) throws IOException, InterruptedException {
// 每个文件传进来时获得文件中属性前缀
            FileSplit input = (FileSplit) context.getInputSplit();
            inputPath = input.getPath().getName();
            try {//获得文件名
                fileCode = inputPath.split("_")[0];
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // System.out.println(fileCode + " : "+value.toString());
            String[] values = value.toString().split(DELIMITER);
            StringBuffer sb = new StringBuffer();
            sb.append(fileCode).append("#");//将文件名拼接到value中，做reduce的判断标识

            if (fileCode.equals(SONGSSDIR)){
                //listener的用户id传入reduce
                sb.append(values[0]);
                context.write(new Text(values[1]),new Text(sb.toString()));
                //System.out.println("songs value : "+values[1]);
            }else{
                for(int i=1;i<values.length;i++){
                sb.append(values[i]).append(DELIMITER);//记录歌的tag
                }
                context.write(new Text(values[0]),new Text(sb.toString()));
                //System.out.println("songs2 value : "+values[0]);
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> left = new ArrayList<String>();
            List<String> right = new ArrayList<String>();
            for (Text value : values) {
                String[] vv = value.toString().split("#");
                String fileCode = vv[0];
                //System.out.println("vv value : "+vv[0]);
                if (fileCode.equals(SONGSSDIR)) {
                    left.add(vv[1]);
                } else {
                    if(vv.length>1) {
                        right.add(vv[1]);
                    }
                }
            }
            System.out.println("user value : "+left.toString()+"  tag "+right.toString());

//只有当left和right都有数据是才会遍历
            for (String l : left) {
                for (String r : right) {
                    context.write(new Text (l),new Text(r));
                }
            }
        }
    }
}