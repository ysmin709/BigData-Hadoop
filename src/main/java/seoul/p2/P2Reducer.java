package seoul.p2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class P2Reducer extends Reducer<Text, Text, Text, Text> {
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        List<String> list = new ArrayList<>();
        int cnt = 0;

        for (Text i : values) {
            StringTokenizer st = new StringTokenizer(i.toString(), ",");
            String time = st.nextToken();

            if (list.contains(time)) cnt++;
            else list.add(time);
        }

        String Count = String.valueOf(cnt);

        ov.set(Count);
        context.write(key, ov);
    }
}