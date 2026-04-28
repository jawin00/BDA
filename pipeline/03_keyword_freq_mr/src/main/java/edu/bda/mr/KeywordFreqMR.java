package edu.bda.mr;

import org.apache.hadoop.conf.Configuration;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hadoop MapReduce job — keyword frequency by year × country.
 *
 * Input:  hdfs:///mr_in/keyword_freq/        (TSV: year\tcountry\ttext, prepared by Spark)
 * Output: hdfs:///mr_out/keyword_freq/       (TSV: year\tcountry\tterm\tcount)
 *
 * Mapper: scan text for any disaster-lexicon term;
 * emit ((year, country, term), 1) per term per document (deduped within doc).
 * Reducer: sum counts.
 *
 * This is the canonical MapReduce deliverable for the syllabus tag.
 */
public class KeywordFreqMR {

    static final Set<String> LEXICON = new HashSet<>(Arrays.asList(
        "earthquake", "quake", "aftershock", "magnitude", "epicenter",
        "tsunami",
        "flood", "flooding", "deluge", "inundation",
        "hurricane", "cyclone", "typhoon", "tornado",
        "wildfire", "bushfire", "blaze",
        "volcano", "eruption", "lava",
        "landslide", "mudslide", "avalanche",
        "drought", "famine",
        "blizzard", "snowstorm",
        "evacuation", "evacuated", "displaced", "refugees",
        "casualties", "fatalities", "injured", "missing",
        "rescue", "relief", "shelter", "aid",
        "damage", "destroyed", "collapsed", "devastated",
        "warning", "alert", "advisory"
    ));

    static final Pattern WORD_RE = Pattern.compile("[a-zA-Z][a-zA-Z\\-]+");

    public static class TokenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            int t1 = line.indexOf('\t');
            if (t1 < 0) return;
            int t2 = line.indexOf('\t', t1 + 1);
            if (t2 < 0) return;

            String year = line.substring(0, t1);
            String country = line.substring(t1 + 1, t2);
            String text = line.substring(t2 + 1).toLowerCase();
            if (year.isEmpty() || country.isEmpty()) return;

            Matcher m = WORD_RE.matcher(text);
            Set<String> seenInDoc = new HashSet<>();
            while (m.find()) {
                String w = m.group();
                if (LEXICON.contains(w) && seenInDoc.add(w)) {
                    outKey.set(year + "\t" + country + "\t" + w);
                    ctx.write(outKey, ONE);
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            total.set(sum);
            ctx.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: KeywordFreqMR <input-tsv-dir> <output-dir>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "keyword-freq-by-year-country");
        job.setJarByClass(KeywordFreqMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }
}
