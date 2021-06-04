package seoul.p1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class P1Reducer extends Reducer<Text, DoubleWritable, Text, Text> {
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
                          Reducer<Text, DoubleWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {

        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        int cnt = 0;

        for(DoubleWritable d : values) {
            double v = d.get();
            if (v < 0) continue;
            if (v < min) min = v;
            if (v > max) max = v;
            sum += d.get();
            cnt++;
        }

        double avg = sum / cnt;

        ov.set(avg + "\t" + max + "\t" + min);
        context.write(key, ov);
    }
}
