package com.gl.reader.service.impl;

import com.gl.reader.constants.Alerts;
import com.gl.reader.model.Book;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gl.reader.dto.Alert.raiseAlert;
import static com.gl.reader.model.Book.createBook;
import static com.gl.reader.service.ProcessController.*;


@Component
public class RecordServiceImpl {
    static Logger logger = LogManager.getLogger(RecordServiceImpl.class);

    public static boolean readRecordsFromFileAndCreateHash(String fileName) {
        Path pathToFile = Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + fileName);
        String line = null;
        String folder_name;
        String file_name = "";
        String event_time;
        String imei = "";
        Date timeStamp;//  (imeiArrival.compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(rs1.getString("update_imei_arrival_time"))) > 0
        String imsi = "";
        String msisdn = "";
        String protocol = "";
        String blackListed = "0";
        try {
            String[] myArray = imeiValCheckMap.get("EDR_IMEI_LENGTH_VALUE").split(",");
            BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
            //int headCount;
            if (sourceName.contains("all")) {
                br.readLine();
                headCount++;
            }
            line = br.readLine(); // need to check why
            while (line != null) {
                itotalCount++; // dec
                totalCount++; // dec
                String[] attributes = line.split(attributeSplitor, -1);
                inputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator

                if (sourceName.contains("all")) {
                    imei = attributes[0];
                    imsi = attributes[1];
                    msisdn = attributes[2];
                    timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(attributes[3]);
                    protocol = attributes[4];
                    folder_name = attributes[5];
                    file_name = attributes[6];
                    event_time = attributes[7];

                } else {
                    imei = attributes[1];  //time -0 , imei-1,imsi -2, msisdn -3 ,protocol-9
                    imsi = attributes[2];
                    msisdn = attributes[3];
                    timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(attributes[0]);
                    folder_name = sourceName;
                    file_name = fileName;
                    event_time = eventTime;
                    blackListed = attributes[6];
                    if (attributes[9].equalsIgnoreCase("SS7"))
                        protocol = "2G";
                    if (attributes[9].equalsIgnoreCase("DIAMETER"))
                        protocol = "4G";
                }
                logger.debug(" Line ----" + imei, imsi, msisdn, timeStamp, protocol);
                Book book = createBook(imei, imsi, msisdn, timeStamp, protocol, folder_name, file_name, event_time);
                {
                    if (blackListed.equalsIgnoreCase("1")) {
                        Book bookBlackListError = createBook(imei, imsi, msisdn, timeStamp, protocol, folder_name, file_name, event_time);
                        if (errorBlacklistFile.contains(bookBlackListError)) {
                            errorBlacklistDuplicate++;
                        } else {
                            inBlacklistErrorSet++;
                            errorBlacklistFile.add(bookBlackListError);
                        }
                        line = br.readLine();
                        blacklisterror++;
                        iBlackListerror++;
                        continue;
                    }
                }
                {
                    if ((imei.isEmpty() || imei.matches("^[0]*$"))) {
                        if (imeiValCheckMap.get("EDR_NULL_IMEI_CHECK").equalsIgnoreCase("true")) {
                            logger.debug("Null Imei ,Check True, Error generator : " + imei);
                            Book bookError = createBook(imei, imsi, msisdn, timeStamp, protocol, folder_name, file_name, event_time);
                            if (errorFile.contains(bookError)) {
                                errorDuplicate++;
                            } else {
                                inErrorSet++;
                                errorFile.add(bookError);
                            }
                            line = br.readLine();
                            error++;
                            ierror++;
                            continue;
                        } else {
                            imei = imeiValCheckMap.get("EDR_NULL_IMEI_REPLACE_PATTERN");
                        }
                    }

                    if (imsi.isEmpty() || msisdn.length() > 20 || (!msisdn.matches("^[ 0-9 ]*$"))
                            || imsi.length() > 20 || (!imsi.matches("^[0-9 ]*$"))
                            || ((imeiValCheckMap.get("EDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true"))
                            && !(Arrays.asList(myArray).contains(String.valueOf(imei.length()))))
                            || (!imei.matches("^[ 0-9 ]+$") && imeiValCheckMap.get("EDR_ALPHANUMERIC_IMEI_CHECK").equalsIgnoreCase("true"))) {
                        logger.debug("Wrong record: imsi/mssidn-> empty, >20, ! 0-9 :: [" + imsi + "][ " + msisdn + "]" + " OR IMEI->When length check defined & length criteria not met,non numeric with alphaNum Check true :[" + imei + "] ");

                        Book bookError = createBook(imei, imsi, msisdn, timeStamp, protocol, folder_name, file_name, event_time);
                        if (errorFile.contains(bookError)) {
                            errorDuplicate++;
                        } else {
                            inErrorSet++;
                            errorFile.add(bookError);
                        }
                        line = br.readLine();
                        error++;
                        ierror++;
                        continue;
                    }
                }

                if (BookHashMap.containsKey(book.getIMEI())) {   // imei already present 1. imsi found 2 . not found :
                    if (!BookHashMap.get(book.getIMEI()).containsKey(book.getIMSI())) {
                        BookHashMap.get(book.getIMEI()).put(book.getIMSI(), book);
                        inSet++;
                        iinSet++;
                        outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                    } else {
                        {  // start here
                            HashMap<String, Book> bookMap1 = BookHashMap.get(book.getIMEI());
                            Book oldBook = bookMap1.get(book.getIMSI());
                            Book newBook = new Book(book.getIMEI(), book.getIMSI(),
                                    oldBook.getMSISDN() == null || oldBook.getMSISDN().isEmpty() ? book.getMSISDN() : oldBook.getMSISDN(),

                                    oldBook.getTimeStamp().after(book.getTimeStamp()) ? book.getTimeStamp() : oldBook.getTimeStamp(),

                                    oldBook.getProtocol().equalsIgnoreCase(book.getProtocol()) ? book.getProtocol() : "2G|4G",

                                    book.getSourceName(), book.getFileName(), book.getEventTime());
                            //   HashMap<String, Book> bookMapNew = new HashMap<>();//  bookMapNew.put(book.getIMSI(), newBook);
                            BookHashMap.get(book.getIMEI()).put(book.getIMSI(), newBook);
                        }  // start here
                        duplicate++;
                        iduplicate++;
                    }

                } else {
                    HashMap<String, Book> bookMap = new HashMap<>();
                    bookMap.put(book.getIMSI(), book);
                    BookHashMap.put(book.getIMEI(), bookMap);
                    inSet++;
                    iinSet++;
                    outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            logger.error("Alert in  " + line + "Error: " + e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(RecordServiceImpl.class.getName())).collect(Collectors.toList()).get(0) + "]");
            raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + ". in file  " + file_name, "<process_name>", "EDR_pre_processor"), 0);
            return false;
        }
        return true;
    }
}

//                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                SimpleDateFormat outputFormat = new SimpleDateFormat("HH:mm:ss");
//
//                try {
//                    Date date = inputFormat.parse(dateString);
//                    String time = outputFormat.format(date);
//
//                  // new SimpleDateFormat("yyyy-MM-dd").parse(book.getTimeStamp())
//                                 .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(oldBook.getTimeStamp())) > 0

