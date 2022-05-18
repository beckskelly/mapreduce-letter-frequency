import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LetterMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	// Letter counter
	enum Letter { WELSH, GALICIAN, NORWEGIAN }
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
    try {
		// assign language to letters counted using file name
    	FileSplit splitFile = (FileSplit)context.getInputSplit();
	    String filename = splitFile.getPath().getName();
	    String lang = filename.substring(0, 3);
		
		String s = value.toString();
		// extract letters and special chars using regular expression
        String[] letters = s.replaceAll("[^a-zA-ZÆæØøÅåÁÉÍÑÓÚÜáéíóúñü]", "").split("(?!^)");

        for (String letter : letters) {
            if (letter.length() > 0) {
            	// write key and value
                context.write(new Text(lang + "_" + letter.toLowerCase()), new FloatWritable(1.0f));
                
                // increment total letter counter
                switch (lang) {
                    case "WEL":
                        context.getCounter(Letter.WELSH).increment(1);
                        break;
                    case "GAL":
                        context.getCounter(Letter.GALICIAN).increment(1);
                        break;
                    case "NOR":
                        context.getCounter(Letter.NORWEGIAN).increment(1);
                        break;
                    default:
                        System.out.println("Not in any of the accepted languages, or your file is incorrectly labelled, error!");
                }
            }
        }
    } catch (Exception e) {
        throw new RuntimeException("Letter Count Error", e);
    }
}
}

