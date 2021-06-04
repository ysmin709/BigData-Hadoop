package seoul.p3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class P3Reducer extends Reducer<Text, Text, Text, Text> {
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();

        for (Text i : values) {
            StringTokenizer st = new StringTokenizer(i.toString(), ",");
            Integer item_code = Integer.parseInt(st.nextToken());
            Double item_value = Double.parseDouble(st.nextToken());

            if (item_code == 1) {
                result.append("  SO2: ");
                result.append(item_value);
            }
            else if (item_code == 3) {
                result.append("  NO2: ");
                result.append(item_value);
            }
            else if (item_code == 5) {
                result.append("  CO: ");
                result.append(item_value);
            }
            else if (item_code == 6) {
                result.append("  O3: ");
                result.append(item_value);
            }
            else if (item_code == 8) {
                result.append("  PM10: ");
                result.append(item_value);
            }
            else if (item_code == 9) {
                result.append("  PM2.5: ");
                result.append(item_value);
            }
        }
        String value = result.toString();

        ov.set(value);
        context.write(key, ov);
    }
}
