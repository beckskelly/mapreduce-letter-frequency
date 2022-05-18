import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FreqMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    try {

		Configuration conf = context.getConfiguration();
		
    	String k = value.toString();
   	    String mykey = k.substring(0, 3);
   	    
		long totalletters = 0;
		
		// totals for each language
        switch (mykey) {
            case "WEL":
            	totalletters = conf.getLong("WELSH_TOTAL", 0);
            case "GAL":
            	totalletters = conf.getLong("GALICIAN_TOTAL", 0);
            case "NOR":
            	totalletters = conf.getLong("NORWEGIAN_TOTAL", 0);
            default:
                System.out.println("Not in any language, error!");
        }
    	FloatWritable count = new FloatWritable();
    	//get individual letter amounts
    	float letterAmts = Float.parseFloat(k.replaceAll("[^0-9.]", ""));
    	
    	// get frequencies
    	float freqs = letterAmts / totalletters;
        count.set(freqs);
        context.write(new Text(value), count);
        
    } catch (Exception e) {
        throw new RuntimeException("Frequency Mapper Error", e);
    }
}
}