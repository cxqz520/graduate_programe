import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class SimilarSongsTag {

    public static final String SongsPath = "part-r-00000";

    public static final String DELIMITER = "    ";

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
//多路径判断
        if (args.length < 2) {
            System.out.println("参数数量不对，至少两个以上参数：<数据文件输出路径>、<输入路径...>");
            System.exit(1);
        }
//输出结果路径
        String dataOutput = args[0];
        String[] inputs = new String[args.length - 1];
        System.arraycopy(args, 1, inputs, 0, inputs.length);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SimilarSongsTag");
        job.setJarByClass(SimilarSongsTag.class);
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
            String songs = new String();//将文件名拼接到value中，做reduce的判断标识
            sb.append(fileCode).append("#");

            if(fileCode.equals(SongsPath)){
                songs = values[1];//similarsongs 第二列 歌曲id作key
                //System.out.println("songs = "+songs);
                sb.append(values[0]);

            }else {
                songs = values[0];//format第一列 歌曲id
                for (int i=1; i<values.length&&i<6;i++){
                    sb.append(values[i]).append(DELIMITER);
                }
                //System.out.println("song2 value : "+songs);
            }
            context.write(new Text(songs),new Text(sb.toString()));

        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> left = new ArrayList<String>();
            List<String> right = new ArrayList<String>();
            for (Text value : values) {
                String[] vv = value.toString().split("#");
                if (vv[0].equals(SongsPath)) {
                    left.add(vv[0]);
                    //System.out.println("value : "+vv[0]);
                } else {
                    if(vv.length>1) {
                        //System.out.println("codefile"+vv[0]+"tag value : " + vv[1].toString());
                        String[] tags = vv[1].split(DELIMITER);
                        for(int i=0;i<tags.length;i++){
                            //System.out.println("tagsi"+tags[i]);
                            right.add(tags[i]);
                        }
                    }
                }
            }
            //System.out.println(right.toString());

            List<String> list = new ArrayList<>();
            for (String l : left) {
                for (String r : right) {
                        context.write(null, new Text(r));
                }
            }

        }
    }
}