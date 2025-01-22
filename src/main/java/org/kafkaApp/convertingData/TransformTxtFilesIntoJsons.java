package org.kafkaApp.convertingData;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.kafkaApp.Structure.entities.DataStructure;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransformTxtFilesIntoJsons {
    private static final Logger LOGGER = Logger.getLogger(TransformTxtFilesIntoJsons.class.getName());
    private static final String filePath = System.getProperty("configFilePath", "C:\\dataset\\ForexStocksTest\\"); // Default path if not provided

    public static String processFile(File file) {
        String fileName = file.getName();
        String fileNameWithoutTheType = fileName.replace(".txt", "");
        String[] partsFileName = fileNameWithoutTheType.split("·");

        String dataSetKey = partsFileName[0];
        String streamID = partsFileName[1];


        try (BufferedReader br = new BufferedReader(new FileReader(file));
             BufferedWriter bw = new BufferedWriter(new FileWriter( filePath + fileNameWithoutTheType+ ".json"))) {
            String line;
            ObjectMapper objectMapper = new ObjectMapper();
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split(",");
                String date = lineParts[0];
                String time = lineParts[1];
                double price = Double.parseDouble(lineParts[2]);
                int volume = Integer.parseInt(lineParts[3]);

                DataStructure dataStructure = new DataStructure(streamID, dataSetKey, date, time, price, volume);
                String jsonString = objectMapper.writeValueAsString(dataStructure);
                bw.write(jsonString);
                bw.newLine();  // Write a newline character after each JSON object
            }

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,"Error when try processing file: "+file.getName(),e);

        }
        return streamID;
    }


    public static void main(String[] args) {
        File folder = new File(filePath);
        FilenameFilter txtFileFilter = (dir, name) -> name.endsWith(".txt");

        File[] files = folder.listFiles(txtFileFilter);
        if (files != null) {
            for (File file : files) {
                System.out.println(file.getName());
                String streamId=processFile(file);
                LOGGER.log(Level.INFO,"Converting Succeed for "+streamId.toUpperCase()+"!!!");

            }
        }else {
            LOGGER.log(Level.WARNING,"Not found files in directory");
        }
    }

}
