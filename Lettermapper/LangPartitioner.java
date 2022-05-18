import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LangPartitioner extends Partitioner<Text, FloatWritable> {
    @Override
    public int getPartition(Text key, FloatWritable values, int numPartitions) {
     	String k = key.toString(); //get key values
     	String mykey = k.substring(0, 3); //partition using substring
        switch (mykey) {
            case "WEL":
                return 0;
            case "GAL":
                return 1;
            case "NOR":
                return 2;
            default:
                return 0;
        }
    }
}