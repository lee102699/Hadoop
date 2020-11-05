import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        Pattern logEntryPattern = Pattern.compile(conf.get("logEntryRegEx"));
        String[] fieldsToCount = conf.get("fieldsToCount").split("");

        String[] entries = value.toString().split("\r?\n");


        for (int i = 0; i < entries.length; i++) {
            Matcher logEntryMatcher = logEntryPattern.matcher(entries[i]);
            if (logEntryMatcher.find()) {
                for (String index : fieldsToCount) {
                    if(!index.equals("")) {
                        Text k = new Text(index + " "
                                + logEntryMatcher.group(Integer.parseInt(index)));
                        context.write(k, one);
                    }
                }
            }
        }
    }
}