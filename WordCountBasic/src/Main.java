import java.io.*;
import java.time.Instant;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        long now = Instant.now().toEpochMilli();
        Map<String, Integer> result = new HashMap<>();
        String row;
        BufferedReader br = new BufferedReader(new FileReader("C:/Users/vucko/Desktop/Flink/bigInputWordCount.txt"));

        while ((row = br.readLine()) != null) {
            String[] parts = row.toLowerCase().split("\\W+");
            for (String word : parts) {
                if (!result.containsKey(word)) {
                    result.put(word, 1);
                } else {
                    result.put(word, result.get(word) + 1);
                }
            }
        }
        br.close();
        result.keySet().forEach(word -> System.out.println("(" + word + "," + result.get(word) + ")"));
        System.out.println((Instant.now().toEpochMilli() - now) / 1000.0 + "s");
    }
}