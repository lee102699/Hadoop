import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        System.out.println("HTTP Log MapReduce job started");

        if (args.length < 1) {
            System.err.println("You must supply the HDFS working directory");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        conf.set(
                "logEntryRegEx",
                "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
        conf.set("fieldsToCount", "1569");

        Job countJob = Job.getInstance(conf);

        countJob.setJarByClass(Main.class);

        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(IntWritable.class);

        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        countJob.setMapperClass(CountMapper.class);
        countJob.setReducerClass(CountReducer.class);

        countJob.setInputFormatClass(TextInputFormat.class);
        countJob.setOutputFormatClass(TextOutputFormat.class);

        countJob.setCombinerClass(CountReducer.class);

        Path inputFile = new Path(args[0] + File.separator + "logs");
        Path countOutput = new Path(args[0] + File.separator + "outputs"
                + File.separator + "counts");

        FileSystem fileSystem = FileSystem.get(conf);
        if (!fileSystem.exists(inputFile)) {
            System.err.println("Input file does not exist! - "
                    + inputFile.getParent());
            return;
        }

        if (fileSystem.exists(countOutput)) {
            fileSystem.delete(countOutput, true);
            System.out
                    .println("Deleted existing output file before continuing.");
        }
        fileSystem.close();

        FileInputFormat.addInputPath(countJob, inputFile);
        FileOutputFormat.setOutputPath(countJob, countOutput);

        countJob.waitForCompletion(true);

        System.out.println("HTTP Log MapReduce job completed");
    }

}