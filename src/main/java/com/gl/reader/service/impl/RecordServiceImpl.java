package com.gl.reader.service.impl;

import com.gl.reader.EdrP1P2Process;
import com.gl.reader.constants.Alerts;
import com.gl.reader.model.Book;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gl.reader.service.ProcessController.*;
import static com.gl.reader.dto.Alert.raiseAlert;
import static com.gl.reader.model.Book.createBook;

public class RecordServiceImpl {
    static Logger logger = LogManager.getLogger(RecordServiceImpl.class);

    public static boolean readRecordsFromFileAndCreateHash(String fileName) {
        logger.info(" get Records from CSV with fileName " + fileName);
        Path pathToFile = Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + fileName);
        String line = null;
        String folder_name;
        String file_name = "";
        String event_time;
        String imei = "";
        String imsi = "";
        String msisdn = "";
        String systemType = "";
        String recordType = "";
        logger.info("File With Path : " + pathToFile);
        try {
            String[] myArray = cdrImeiCheckMap.get("CDR_IMEI_LENGTH_VALUE").split(",");
            BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
            //int headCount;
            if (sourceName.equals("all")) {
                br.readLine();
                headCount++;
            }
            line = br.readLine();
            while (line != null) {
                itotalCount++; // dec
                totalCount++; // dec
                logger.info("Actual LINE--: " + line);
                String[] attributes = line.split(attributeSplitor, -1);
                if (attributes.length < 5) {// return error move line to error  + add conters // go to next line
                    logger.info("Line length is less");
                }
                inputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                if (sourceName.equals("all")) {
                    folder_name = attributes[5];
                    file_name = attributes[6];
                    event_time = attributes[7];
                } else {
                    folder_name = sourceName;
                    file_name = fileName;
                    event_time = eventTime;
                }
                if (ims_sources.contains(sourceName)) {
                    if (attributes[0].equalsIgnoreCase("role-of-Node") || attributes[0].equalsIgnoreCase("role_of_Node")) {    //role_of_Node
                        line = br.readLine();
                        headCount++;
                        continue;
                    }
                    if (attributes[1].equalsIgnoreCase("IMEI")) {
                        imei = attributes[2].replaceAll("-", "");  //.substring(0, 14)
                        if (attributes[3].toLowerCase().contains("imsi")) {
                            imsi = attributes[4];
                        }
                        if ("6".equals(attributes[9])) {
                            msisdn = attributes[10].replace("tel:+", "");
                        } else {
                            if ("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0])) {
                                msisdn = attributes[5].replace("tel:+", "");
                            } else if ("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0])) {
                                msisdn = attributes[6].replace("tel:+", "");
                            }
                        }
                        String[] systemTypeTemp = attributes[7].split(propertiesReader.semiColonDelimiter, -1);
                        systemType = systemTypeTemp[0];
                        if (("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0]))
                                && ("INVITE".equals(attributes[8]) || "BYE".equals(attributes[8]))) {
                            recordType = "0";
                        } else if (("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0]))
                                && ("INVITE".equals(attributes[8]) || "BYE".equals(attributes[8]))) {
                            recordType = "1";
                        } else if (("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0]))
                                && "MESSAGE".equals(attributes[8])) {
                            recordType = "6";
                        } else if (("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0]))
                                && "MESSAGE".equals(attributes[8])) {
                            recordType = "7";
                        } else {
                            recordType = "100";
                        }
                    } else {
                        line = br.readLine();
                        error++;
                        ierror++;
                    }
                } else {
                    if (attributes[0].equalsIgnoreCase("IMEI")) {
                        headCount++;
                        line = br.readLine();
                        continue;
                    }
                    imei = attributes[0];
                    imsi = attributes[1];
                    msisdn = attributes[2];
                    recordType = attributes[3];
                    systemType = attributes[4];
                }
                logger.info("CDR Line ----" + imei, imsi, msisdn, recordType, systemType);
                Book book = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);

                if ((imei.isEmpty() || imei.matches("^[0]*$"))) {
                    if (cdrImeiCheckMap.get("CDR_NULL_IMEI_CHECK").equalsIgnoreCase("true")) {
                        logger.info("Null Imei ,Check True, Error generator : " + imei);
                        Book bookError = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);
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
                        imei = cdrImeiCheckMap.get("CDR_NULL_IMEI_REPLACE_PATTERN");
                        logger.info("Null Imei and Check  is False, now Converting  imei :" + imei);
                    }
                }


                if (imsi.isEmpty() || msisdn.isEmpty()
                        || imsi.length() > 20 || msisdn.length() > 20 || (!imsi.matches("^[a-zA-Z0-9_]*$")) || (!msisdn.matches("^[a-zA-Z0-9_]*$"))
                        || ((cdrImeiCheckMap.get("CDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true"))
                        && !(Arrays.asList(myArray).contains(String.valueOf(imei.length()))))
                        || (!imei.matches("^[ 0-9 ]+$") && cdrImeiCheckMap.get("CDR_ALPHANUMERIC_IMEI_CHECK").equalsIgnoreCase("true"))
                ) {
                    logger.info("Wrong record: imsi/mssidn-> empty, >20, !a-Z0-9 :: [" + imsi + "][ " + msisdn + "]"
                            + " OR imei->When length check defined & length criteria not met,non numeric with alphaNum Check true :[" + imei + "] ");
                    Book bookError = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);
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


//                if (!reportTypeSet.isEmpty()) { // set is empty
//                    if (!reportTypeSet.contains(recordType)) {
//                        line = br.readLine();
//                        error++;
//                        totalCount++;
//                        ierror++;
//                        itotalCount++;
//                        continue;
//                    }
//                }
                if (BookHashMap.containsKey(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI())) {
                    if (!BookHashMap.get(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI()).containsKey(book.getMSISDN())) {
                        BookHashMap.get(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI()).put(book.getMSISDN(), book);
                        inSet++;
                        iinSet++;
                        outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                    } else {
                        duplicate++;
                        iduplicate++;
                    }
                } else {
                    HashMap<String, Book> bookMap = new HashMap<>();
                    bookMap.put(book.getMSISDN(), book);
                    BookHashMap.put(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI(), bookMap);
                    // logger.info("If no imei then object: " + book);
                    inSet++;
                    iinSet++;
                    outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            logger.error("Alert in  " + line + "Error: " + e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(EdrP1P2Process.class.getName())).collect(Collectors.toList()).get(0) + "]");
            raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + ". in file  " + file_name, "<process_name>", "CDR_pre_processor"), 0);
            return false;
        }
        return true;
    }
}
