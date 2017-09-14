import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class GetUser {


    public static final String DELIMITER = "    ";
    public static final String SONGSSDIR = "result1.txt";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {



        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GetUser");
        job.setJarByClass(GetUser.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (args.length < 2) {
            System.out.println("请输入：<数据文件输出路径>、<输入路径1>、<输入路径2>");
            System.exit(1);
        }
        String dataOutput = args[0];
        String[] inputs = new String[args.length - 1];
        System.arraycopy(args, 1, inputs, 0, inputs.length);
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
            try {
//获得文件名
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
//将文件名拼接到value中，做reduce的判断标识
            sb.append(fileCode).append("#");

            if (fileCode.equals(SONGSSDIR)){
                sb.append("exist");
                //System.out.println("song value : "+fileCode);
            }else{
                sb.append(values[0]);
                //System.out.println("listen value : "+fileCode+"values0"+values[1]);
            }
            context.write(new Text(values[1]), new Text(sb.toString()));

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
                System.out.println("vv value : "+vv[0]);
                if (fileCode.equals(SONGSSDIR)) {
                    left.add("exist");
                } else {
                    right.add(vv[1]);
                }
            }

//只有当left和right都有数据是才会遍历
            for (String l : left) {
                for (String r : right) {
                    context.write(new Text (),new Text(r));
                }
            }
        }
    }
}