package seoul.p3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P3Mapper extends Mapper<Object, Text, Text, Text> {
    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(value.toString(), ",");
        String time = st.nextToken();
        String station_code = st.nextToken();
        String item_code = st.nextToken();
        Double item_value = Double.parseDouble(st.nextToken());
        Integer check = Integer.parseInt(st.nextToken());

        if (check == 0) {
            ok.set(time + " " + station_code);
            ov.set(item_code + "," + item_value);
            context.write(ok, ov);
        }
    }
}
