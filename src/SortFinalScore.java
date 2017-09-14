import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import util.Common;
import util.StringSort;



public class SortFinalScore {

    public static final String UserPath = "user.txt";

    public static final String DELIMITER = "    ";

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
//多路径判断
        if (args.length < 2) {
            System.out.println("参数数量不对，至少两个以上参数：<数据文件输出路径>、<输入路径...>");
            System.exit(1);
        }
//输出结果路径
        String dataOutput = args[0];
//
        String[] inputs = new String[2];
        inputs[0] = Common.AimSongsInput1;
        inputs[1] = Common.AimSongsInput2;


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortFinalScore");
        job.setJarByClass(SortFinalScore.class);
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
            try {
//获得文件名
                fileCode = inputPath.split("_")[0];
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split(DELIMITER);
            StringBuffer sb = new StringBuffer();
//将文件名拼接到value中，做reduce的判断标识
            sb.append(fileCode).append("#");
            if(fileCode.equals(UserPath)){
                String user = values[0].split("\t")[0];
                //System.out.println("user = "+values[0]);
                sb.append(values[0]);

            }else {
                sb.append(values[1]).append(DELIMITER).append(values[2]);
                //System.out.println("user value : "+values[0]);
            }
            //System.out.println("user value : "+sb.toString());
            context.write(new Text(values[0]), new Text(sb.toString()));

        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> left = new ArrayList<String>();
            List<String> right = new ArrayList<String>();
            for (Text value : values) {
                //System.out.println("value : "+value);
                String[] vv = value.toString().split("#");
                if (vv[0].equals(UserPath)) {
                    left.add(vv[0]);
                } else {
                    //System.out.println("vv1 value : "+vv[1].toString());
                    right.add(vv[1]);
                }
            }
            List<StringSort> list = new ArrayList<>();
            for (String l : left) {
                for (String r : right) {
                    //System.out.println(r.split(DELIMITER)[1]);
                    list.add(new StringSort(r,"    ",1));
                }
            }
            Collections.sort(list);
            for(int i = 0;i<20 && i<list.size();i++){
                String str = list.get(i).toString().split(DELIMITER)[0];
                context.write(null,new Text(str));
            }
        }
    }
}