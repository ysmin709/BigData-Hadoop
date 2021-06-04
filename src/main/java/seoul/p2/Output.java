package seoul.p2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class Output {
    public static void main(String[] args) throws IOException {
        String inputPath = "data.csv.out/part-r-00000";
        BufferedReader br = new BufferedReader(new FileReader(inputPath));

        String line = null;
        String station = "";
        int max = 0;

        while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line, "\t");
            String station_code = st.nextToken();
            Integer value = Integer.parseInt(st.nextToken());

            if (max < value) {
                station = station_code;
                max = value;
            }
        }
        System.out.println("공기의 질이 \"좋음\" 수준이 가장 많이 측정된 지역은 " + station + "입니다.");
    }
}
