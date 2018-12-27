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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by juns on 2018/12/26
 * 统计每个小时访问量
 * map: {key:$time_local,value:1}
 * reduee:{key$time_local, value:sum()}
 */
public class KPITime {
    public static class TimeMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text time = new Text();
        private IntWritable one = new IntWritable(1);

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterTime(value.toString());
            if (kpi.getTime_local() != null){
                time.set(kpi.getTime_local_hour());
                context.write(time,one);
            }
        }
    }

    public static class TimeReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable counts = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values){
                sum +=v.get();
            }
            counts.set(sum);
            context.write(key,counts);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Path inPath = new Path("/dg-hadoop/access.log.10");
        Path outPath = new Path("/dg-hadoop/time/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("count Times");
        job.setJarByClass(KPITime.class);

        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.setMapperClass(TimeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TimeReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
