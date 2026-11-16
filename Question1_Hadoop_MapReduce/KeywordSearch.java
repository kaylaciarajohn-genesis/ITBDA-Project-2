
package za.co.mock.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeywordSearch extends Configured implements Tool {

    // Read each complete document as one record
    public static class WholeFileInputFormat
            extends FileInputFormat<NullWritable, BytesWritable> {

        @Override
        public RecordReader<NullWritable, BytesWritable>
                createRecordReader(InputSplit split,
                                   TaskAttemptContext context) {
            return new WholeFileRecordReader();
        }
    }

    public static class WholeFileRecordReader
            extends RecordReader<NullWritable, BytesWritable> {

        private BytesWritable value = new BytesWritable();
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split,
                               TaskAttemptContext context)
                throws IOException {

            FileSplit fileSplit = (FileSplit) split;
            Path path = fileSplit.getPath();

            FileSystem fs = path.getFileSystem(
                    context.getConfiguration());

            int length = (int) fileSplit.getLength();
            byte[] contents = new byte[length];

            try (FSDataInputStream in = fs.open(path)) {
                in.readFully(0, contents);
            }

            value.set(contents, 0, contents.length);
        }

        @Override
        public boolean nextKeyValue() {
            if (!processed) {
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public BytesWritable getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() {
        }
    }

    // Mapper: search for the keyword and record its position
    public static class SearchMapper
            extends Mapper<NullWritable, BytesWritable,
                           Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(NullWritable key,
                           BytesWritable value,
                           Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            String keyword = conf.get("search.keyword", "").trim();

            if (keyword.isEmpty()) {
                throw new IOException("No search keyword supplied.");
            }

            String document = new String(
                    value.getBytes(), 0, value.getLength(),
                    java.nio.charset.StandardCharsets.UTF_8);

            FileSplit split = (FileSplit) context.getInputSplit();
            String filename = split.getPath().getName();

            String regex =
                    "(?<![\\p{L}\\p{N}_])" +
                    Pattern.quote(keyword) +
                    "(?![\\p{L}\\p{N}_])";

            Pattern pattern = Pattern.compile(
                    regex,
                    Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);

            String[] lines = document.split("\\R", -1);

            for (int row = 0; row < lines.length; row++) {

                Matcher matcher = pattern.matcher(lines[row]);

                while (matcher.find()) {

                    int column = lines[row]
                            .codePointCount(0, matcher.start()) + 1;

                    String position = filename + "\t"
                            + (row + 1) + "\t" + column;

                    context.write(new Text(position), one);
                }
            }
        }
    }

    // Reducer: count matches at each position
    public static class SearchReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            context.write(key, new IntWritable(total));
        }
    }

    // Hadoop job configuration
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: KeywordSearch <input> <output>");
            return 2;
        }

        Configuration conf = getConf();

        if (conf.get("search.keyword") == null ||
                conf.get("search.keyword").trim().isEmpty()) {
            System.err.println("Please supply -Dsearch.keyword=value");
            return 2;
        }

        Job job = Job.getInstance(conf, "Keyword Search");

        job.setJarByClass(KeywordSearch.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(
                new Configuration(),
                new KeywordSearch(),
                args);

        System.exit(result);
    }
}
