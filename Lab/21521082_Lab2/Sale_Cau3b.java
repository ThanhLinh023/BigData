import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sale_Cau3b {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\\s+");
            for (int i = 0; i < items.length; i++) {
                word.set(items[i]);
                context.write(word, one);
                for (int j = i + 1; j < items.length; j++) {
                    word.set(items[i] + "," + items[j]);
                    context.write(word, one);
                }
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private Map<String, Integer> productCountMap = new HashMap<>();
        private FloatWritable result = new FloatWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String keyStr = key.toString();

            if (keyStr.contains(",")) {
                productCountMap.put(keyStr, sum);
            } else {
                productCountMap.put(keyStr, sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : productCountMap.entrySet()) {
                String pair = entry.getKey();

                if (pair.contains(",")) {
                    String[] items = pair.split(",");
                    String itemA = items[0];
                    int countA = productCountMap.getOrDefault(itemA, 1);
                    int pairCount = entry.getValue();
                    float probability = (float) pairCount / countA;
                    result.set(probability);
                    context.write(new Text(pair + ": "), result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sale probability");

        job.setJarByClass(Sale_Cau3b.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
