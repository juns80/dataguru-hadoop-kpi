package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by juns on 2018/12/26
 * 统计给定的request里面的各自访问量
 *    map: {key: $request, value:1}
 *   reduce: {key: $request, value:sum()}
 */
public class KPIPV {
    public KPIPV() {}

    public static class PVMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text pv = new Text();
        private IntWritable count = new IntWritable(1);

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isVaild()){
                pv.set(kpi.getRequest());
                context.write(pv,count);
            }
        }
    }

    public static class PVReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values){
                sum+=v.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Path inPath = new Path("/dg-hadoop/access.log.10");
        Path outPath = new Path("/dg-hadoop/pv/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("count PVs");
        job.setJarByClass(KPIPV.class);

        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.setMapperClass(PVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(PVReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
