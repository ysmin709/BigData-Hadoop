package seoul.p1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
    Text ok = new Text();
    DoubleWritable ov = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
            throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(value.toString(), ",");
        st.nextToken();
        String station_code = st.nextToken();
        String item_code = st.nextToken();
        double item_value = Double.parseDouble(st.nextToken());
        Integer check = Integer.parseInt(st.nextToken());

        if (check == 0) {
            ok.set(station_code + "\t" + item_code);
            ov.set(item_value);
            context.write(ok, ov);
        }
    }
}
