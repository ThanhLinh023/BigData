import java.io.IOException;
import java.util.StringTokenizer;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndustryTrade {
    // public static class CountByNgayCapNhatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //     private Text date = new Text();
    //     private IntWritable count = new IntWritable(1);
    //     private double threshold;

    //     @Override
    //     protected void setup(Context context) throws IOException, InterruptedException {
    //         threshold = context.getConfiguration().getDouble("price.threshold", 0.0);
    //     }

    //     @Override
    //     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //         String line = value.toString();
    //         String[] fields = line.split(",");

    //         if (fields.length >= 4) {
    //             try {
    //                 double price = Double.parseDouble(fields[1].replace("\"", "").trim());
    //                 String updateDate = fields[3].replace("\"", "").trim();
    //                 if (price > threshold) {
    //                     date.set(updateDate);
    //                     context.write(date, count);
    //                 }
    //             } 
    //             catch (NumberFormatException e) 
    //             {
    //             }
    //         }
    //     }
    // }
    // public static class CountByNgayCapNhatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //     private IntWritable result = new IntWritable();

    //     @Override
    //     protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    //         int sum = 0;
    //         for (IntWritable val : values) {
    //             sum += val.get();
    //         }
    //         result.set(sum);
    //         context.write(key, result);
    //     }
    // }
    // public static class AvgPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    //     @Override
    //     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //         String line = value.toString();
    //         String[] fields = line.split(",");
    //         if (fields.length == 4) {
    //             String ten = fields[0];
    //             try
    //             {
    //                 double gia = Double.parseDouble(fields[1].replace("\"", "").trim());
    //                 context.write(new Text(ten), new DoubleWritable(gia));
    //             } catch (NumberFormatException e)
    //             {
                    
    //             }
    //         }
    //         else
    //         {
    //             System.err.println("line: " + line);
    //         }
    //     }
    // }
    // public static class AvgPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    //     @Override
    //     public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    //         double sum = 0;
    //         int count = 0;
    //         for (DoubleWritable value : values) {
    //             sum += value.get();
    //             count++;
    //         }
    //         context.write(key, new DoubleWritable(sum / count));
    //     }
    // }
    public static class MinMaxPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length == 4) {
                String ten = fields[0];
                try
                {
                    double gia = Double.parseDouble(fields[1].replace("\"", "").trim());
                    context.write(new Text(ten), new DoubleWritable(gia));
                } catch (NumberFormatException e)
                {
                    
                }
            }
            else
            {
                System.err.println("line: " + line);
            }
        }
    }
    public static class MinMaxPriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;
        
            for (DoubleWritable value : values) {
                double price = value.get();
                if (price < minPrice) {
                    minPrice = price;
                }
                if (price > maxPrice) {
                    maxPrice = price;
                }
            }
            context.write(key, new Text("Max: " + maxPrice + ", Min: " + minPrice));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // if (args.length < 3) {
        //     System.err.println("Usage: TradeCount <input path> <output path> <price threshold>");
        //     System.exit(-1);
        // }
        // Configuration conf = new Configuration();
        // conf.setDouble("price.threshold", Double.parseDouble(args[2]));
        // Job job1 = Job.getInstance(conf, "Industry Trade");
        // job1.setJarByClass(IndustryTrade.class);
        // job1.setMapperClass(CountByNgayCapNhatMapper.class);
        // job1.setReducerClass(CountByNgayCapNhatReducer.class);
        // job1.setOutputKeyClass(Text.class);
        // job1.setOutputValueClass(IntWritable.class);
        // FileInputFormat.addInputPath(job1, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        // System.exit(job1.waitForCompletion(true) ? 0 : 1);

        // // Cau b
        // Job job2 = Job.getInstance(conf, "Average price per product");
        // job2.setJarByClass(IndustryTrade.class);
        // job2.setMapperClass(AvgPriceMapper.class);
        // job2.setReducerClass(AvgPriceReducer.class);
        // job2.setOutputKeyClass(Text.class);
        // job2.setOutputValueClass(DoubleWritable.class);
        // FileInputFormat.addInputPath(job2, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_avg"));
        // System.exit(job2.waitForCompletion(true) ? 0 : 1);

        // // Cau c
        Job job3 = Job.getInstance(conf, "Max and Min price per product");
        job3.setJarByClass(IndustryTrade.class);
        job3.setMapperClass(MinMaxPriceMapper.class);
        job3.setReducerClass(MinMaxPriceReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_minmax"));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}