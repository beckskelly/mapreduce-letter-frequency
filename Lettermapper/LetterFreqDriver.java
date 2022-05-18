import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterFreqDriver {
    public static void main(String[] args) throws Exception {
    // check that there is an input and output path specified
    if (args.length != 2) {
        System.err.println("Usage: LetterFreqDriver <input path> <output path>");
        System.exit(-1);
    }

    try {
        Configuration conf = new Configuration();

        System.out.println("In Driver now!");
        
        //get letter counts
		Job countLetters = Job.getInstance();
        countLetters.setJarByClass(LetterFreqDriver.class);
        countLetters.setJobName("LetterCount");
        countLetters.setMapperClass(LetterMapper.class);
        countLetters.setCombinerClass(SumReducer.class);
        countLetters.setReducerClass(SumReducer.class);
        countLetters.setNumReduceTasks(3);
        countLetters.setPartitionerClass(LangPartitioner.class);
        countLetters.setMapOutputKeyClass(Text.class);
        countLetters.setMapOutputValueClass(FloatWritable.class);
        countLetters.setOutputKeyClass(Text.class);
        countLetters.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(countLetters, new Path(args[0]));//input path    
        FileOutputFormat.setOutputPath(countLetters, new Path(args[1] + "/lettercounts/"));//output of job

        countLetters.waitForCompletion(true);

		//letter totals for frequency calc. Taken from LetterMapper counter
    	long WELSH_TOTAL = countLetters.getCounters().findCounter("LetterMapper$Letter", "WELSH").getValue();
    	long GALICIAN_TOTAL = countLetters.getCounters().findCounter("LetterMapper$Letter", "GALICIAN").getValue();
    	long NORWEGIAN_TOTAL = countLetters.getCounters().findCounter("LetterMapper$Letter", "NORWEGIAN").getValue();
        conf.setLong("WELSH_TOTAL", WELSH_TOTAL);//set total in config
        conf.setLong("GALICIAN_TOTAL", GALICIAN_TOTAL);
        conf.setLong("NORWEGIAN_TOTAL", NORWEGIAN_TOTAL);

        //calculate frequencies
    	Job GetFreqs = Job.getInstance(conf, "GetFreqs");
        GetFreqs.setJarByClass(LetterFreqDriver.class);
        GetFreqs.setJobName("LetterFrequency");
        GetFreqs.setNumReduceTasks(3);
        GetFreqs.setMapperClass(FreqMapper.class);
        GetFreqs.setCombinerClass(FreqReducer.class);
        GetFreqs.setReducerClass(FreqReducer.class);
        GetFreqs.setPartitionerClass(LangPartitioner.class);
        GetFreqs.setMapOutputKeyClass(Text.class);
        GetFreqs.setMapOutputValueClass(FloatWritable.class);
        GetFreqs.setOutputKeyClass(Text.class);
        GetFreqs.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(GetFreqs, new Path(args[1] + "/lettercounts/"));//output of countLetters
        FileOutputFormat.setOutputPath(GetFreqs, new Path(args[1] + "/freqs/"));//output of getFreqs

        //exit when GetFreqs job complete
        System.exit(GetFreqs.waitForCompletion(true) ? 0 : 1);

    } catch (IOException e) {
        e.printStackTrace();
    }
}
}
