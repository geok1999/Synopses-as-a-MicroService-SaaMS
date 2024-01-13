package org.kafkaApp.transformBigData;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JsonPriceCounter {
    public static void main(String[] args) {
        String filePath = "C:\\dataset\\ForexEURTRYNoExpiry.json";
        double specificPrice =6.18177;
        int specificVolume = 1;
        System.out.println(countPriceOccurrences(filePath, specificPrice,specificVolume));

        //System.out.println("The specific price " + specificPrice + " appears " + count + " times.");
    }

    private static String countPriceOccurrences(String filePath, double specificPrice,int specificVolume) {
        int countPrince = 0;
        int countVolume = 0;

        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            JSONArray jsonArray = new JSONArray(content);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                double price = jsonObject.getDouble("price");
                double volume = jsonObject.getDouble("volume");

                if (price == specificPrice) {
                    countPrince++;
                }if (volume == specificVolume) {
                    countVolume++;
                }

            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        return "the count price "+ Double.toString(countPrince)+","+"the count volume"+Integer.toString(countVolume);
    }
}
