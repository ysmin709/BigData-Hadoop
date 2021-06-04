package seoul.p4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class P4Reducer extends Reducer<Text, Text, Text, Text> {
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        int cnt1 = 0, cnt3 = 0, cnt5 = 0, cnt6 = 0, cnt8 = 0, cnt9 = 0;
        double sum1 = 0, sum3 = 0, sum5 = 0, sum6 = 0, sum8 = 0, sum9 = 0;

        for (Text i : values) {
            StringTokenizer st = new StringTokenizer(i.toString(), ",");
            Integer item_code = Integer.parseInt(st.nextToken());
            Double item_value = Double.parseDouble(st.nextToken());

            if (item_code == 1) { cnt1++; sum1 += item_value; }
            else if (item_code == 3) { cnt3++; sum3 += item_value; }
            else if (item_code == 5) { cnt5++; sum5 += item_value; }
            else if (item_code == 6) { cnt6++; sum6 += item_value; }
            else if (item_code == 8) { cnt8++; sum8 += item_value; }
            else if (item_code == 9) { cnt9++; sum9 += item_value; }
        }
        Double avg1 = sum1 / cnt1;
        Double avg3 = sum3 / cnt3;
        Double avg5 = sum5 / cnt5;
        Double avg6 = sum6 / cnt6;
        Double avg8 = sum8 / cnt8;
        Double avg9 = sum9 / cnt9;

        String result = "  SO2: " + avg1.toString() + "  NO2: " + avg3 + "  CO: " + avg5 +
                "  O3: " + avg6 + "  PM10: " + avg8 + "  PM2.5: " + avg9;

        ov.set(result);
        context.write(key, ov);
    }
}