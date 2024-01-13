package org.kafkaApp.transformBigData;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.kafkaApp.Structure.DataStructure;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PreProcessDataBeforeUsed {
    private static int sequenceCounter = 0;  // Static counter variable
    public static String processFile(File file) {
        String fileName = file.getName();
        String[] nameParts = fileName.split("·");

        String[] helpConstructStream = nameParts[1].split("\\.");


        String dataSetKey = nameParts[0]+helpConstructStream[1];
        String streamID = helpConstructStream[0];
        /*String dataSetKey = nameParts[0];
        String streamID = nameParts[1];
        */

        // Assuming objectID is not used as per your description

        List<DataStructure> dataStructures = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file));
             BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\dataset\\\\United Kingdom\\" + fileName.replace(".txt", "") + ".json"))) {
            String line;
            ObjectMapper objectMapper = new ObjectMapper();
            while ((line = br.readLine()) != null) {
                String[] lineParts = line.split(",");
                String date = lineParts[0];
                String time = lineParts[1];
                double price = Double.parseDouble(lineParts[2]);
                int volume = Integer.parseInt(lineParts[3]);
                String objectID = streamID + "_" + sequenceCounter;
                sequenceCounter++;

                DataStructure dataStructure = new DataStructure(streamID, objectID, dataSetKey, date, time, price, volume);
                String jsonString = objectMapper.writeValueAsString(dataStructure);
                bw.write(jsonString);
                bw.newLine();  // Write a newline character after each JSON object
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return streamID;
    }
    private static void writeToExcel(String stockName, int sequenceCount, String excelFilePath) {
        Workbook workbook;
        Sheet sheet;
        File file = new File(excelFilePath);

        try {
            if (file.exists()) {
                // If the file already exists, open and modify it
                FileInputStream inputStream = new FileInputStream(file);
                workbook = WorkbookFactory.create(inputStream);
                sheet = workbook.getSheetAt(0);
            } else {
                // If the file does not exist, create a new workbook and sheet
                workbook = new XSSFWorkbook();
                sheet = workbook.createSheet("Stock Data");
            }

            // Determine the next available row
            int rowCount = sheet.getLastRowNum();
            if (rowCount >= 0 && sheet.getRow(rowCount) != null) {
                rowCount += 1;
            }

            // Create a new row and write data
            Row row = sheet.createRow(rowCount);
            Cell nameCell = row.createCell(0);
            nameCell.setCellValue(stockName);
            Cell counterCell = row.createCell(1);
            counterCell.setCellValue(sequenceCount);

            // Write changes to the file
            FileOutputStream outputStream = new FileOutputStream(excelFilePath);
            workbook.write(outputStream);
            workbook.close();
            outputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        File folder = new File("C:\\dataset\\\\United Kingdom\\");
        FilenameFilter txtFileFilter = (dir, name) -> name.endsWith(".txt");

        File[] files = folder.listFiles(txtFileFilter);
        if (files != null) {
            for (File file : files) {
                System.out.println(file.getName());
                String streamId=processFile(file);
                writeToExcel(streamId, sequenceCounter, "C:\\dataset\\StreamsValidation.xlsx");
                System.out.println(sequenceCounter );
                sequenceCounter = 0; // Reset counter for next file
            }
        }
    }


}
