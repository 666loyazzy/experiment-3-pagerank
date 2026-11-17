import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMR {
    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        private void emit(Context context, String key, String value) throws IOException, InterruptedException {
            context.write(new Text(key), new Text(value));
        }

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t", -1);
            boolean first = context.getConfiguration().getBoolean("rank.first", false);
            String page = fields[0];
            double rank = first ? 1.0 : Double.parseDouble(fields[1]);
            String links = fields[first ? 1 : 2];
            emit(context, page, "S\t" + links);
            if (links.isEmpty()) {
                emit(context, "!", Double.toString(rank));
            } else {
                String[] targets = links.split(",");
                for (String target : targets) emit(context, target, "R\t" + rank / targets.length);
            }
        }
    }

    public static class RankReducer extends Reducer<Text, Text, Text, NullWritable> {
        private double dangling;

        public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (page.toString().equals("!")) {
                for (Text value : values) dangling += Double.parseDouble(value.toString());
                return;
            }
            String links = null;
            double incoming = 0.0;
            for (Text value : values) {
                String record = value.toString();
                if (record.startsWith("S\t")) links = record.substring(2);
                else incoming += Double.parseDouble(record.substring(2));
            }
            if (links == null) throw new IOException("Page missing from input: " + page);
            int pages = context.getConfiguration().getInt("rank.pages", 0);
            double rank = 0.15 + 0.85 * (incoming + dangling / pages);
            String result = context.getConfiguration().getBoolean("rank.final", false)
                    ? String.format(Locale.ROOT, "(%s,%.10f)", page, rank)
                    : page + "\t" + Double.toString(rank) + "\t" + links;
            context.write(new Text(result), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("PageRankMR <input> <output> <pages>");
        int pages = Integer.parseInt(args[2]);
        if (pages <= 0) throw new IllegalArgumentException("Page count must be positive");
        Path input = new Path(args[0]);
        for (int iteration = 1; iteration <= 10; iteration++) {
            Configuration conf = new Configuration();
            conf.setInt("rank.pages", pages);
            conf.setBoolean("rank.first", iteration == 1);
            conf.setBoolean("rank.final", iteration == 10);
            Job job = Job.getInstance(conf, "PageRank iteration " + iteration);
            job.setJarByClass(PageRankMR.class);
            job.setMapperClass(RankMapper.class);
            job.setReducerClass(RankReducer.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            Path output = new Path(args[1], String.format("iteration-%02d", iteration));
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            if (!job.waitForCompletion(true)) System.exit(1);
            input = output;
        }
    }
}
