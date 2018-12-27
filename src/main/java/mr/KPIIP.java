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
import java.util.HashSet;
import java.util.Set;

/**
 * Created by juns on 2018/12/26
 * 一个地址有多少个ip访问
 * map: {key:$request, value:$remote_addr}
 * reduce: {key:$request, value:去重求和(sum(unique))}
 */
public class KPIIP {
    public static class IPMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text request = new Text();
        private Text ip = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isVaild()) {
                request.set(kpi.getRequest());
                ip.set(kpi.getRemote_addr());
                context.write(request, ip);
            }
        }
    }

    public static class IPReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable counts = new IntWritable();
        Set<String> ips = new HashSet<String>();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                ips.add(t.toString());
            }
            counts.set(ips.size());
            context.write(key, counts);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path inPath = new Path("/dg-hadoop/access.log.10");
        Path outPath = new Path("/dg-hadoop/ip/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("count IPs");
        job.setJarByClass(KPIIP.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(IPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IPReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
