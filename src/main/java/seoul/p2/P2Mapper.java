package seoul.p2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class P2Mapper extends Mapper<Object, Text, Text, Text> {
    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString(), ",");
        String time = st.nextToken();
        String station_code = st.nextToken();
        Integer item_code = Integer.parseInt(st.nextToken());
        Double item_value = Double.parseDouble(st.nextToken());
        Integer check = Integer.parseInt(st.nextToken());

        if (!(item_code == 8 || item_code == 9)) return;
        if (item_code == 8) if (item_value > 30) return;
        if (item_code == 9) if (item_value > 15) return;

        if (check == 0) {
            ok.set(station_code);
            ov.set(time + "," + item_code + "," + item_value);
            context.write(ok, ov);
        }
    }
}