import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingCards {

    public static class CardMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text card = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                card.set(line);
                context.write(card, one);
            }
        }
    }

    public static class CardReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        private static final String[] SUITS = {"H", "D", "C", "S"};
        private static final String[] RANKS = {
                "A", "2", "3", "4", "5", "6", "7",
                "8", "9", "10", "J", "Q", "K"
        };

        private Set<String> seenCards = new HashSet<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            seenCards.add(key.toString());
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Set<String> fullDeck = new HashSet<>();
            for (String suit : SUITS) {
                for (String rank : RANKS) {
                    fullDeck.add(rank + suit);
                }
            }

            for (String card : fullDeck) {
                if (!seenCards.contains(card)) {
                    context.write(new Text(card), NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Missing Poker Cards");

        job.setJarByClass(MissingCards.class);
        job.setMapperClass(CardMapper.class);
        job.setReducerClass(CardReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
