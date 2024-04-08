package com.gl.reader;

import com.gl.reader.model.Book;
import com.gl.reader.configuration.ConnectionConfiguration;
import com.gl.reader.configuration.PropertiesReader;
import com.gl.reader.constants.Alerts;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.*;

@EnableAsync
@SpringBootConfiguration
@SpringBootApplication(scanBasePackages = {"com.gl.reader"})
@EnableEncryptableProperties
// working code, if code not working use this
public class FileReaderHashApplicationWorking05012024 {

    static Logger logger = LogManager.getLogger(FileReaderHashApplicationWorking05012024.class);

    static long duplicate = 0;
    static long error = 0;
    static long inSet = 0;
    static long totalCount = 0;
    static long iduplicate = 0;
    static long ierror = 0;
    static long iinSet = 0;
    static long itotalCount = 0;
    static String type;
    static long value;
    static long processed = 0;
    static String fileName;
    static String extension;
    static String servername;
    static Integer sleep;
    static String folderName;
    static String sourceName;
    static String eventTime;
    static String errorFlag;
    static long errorDuplicate = 0;
    static long inErrorSet = 0;
    static long totalFileCount = 0;
    static long totalFileRecordsCount = 0;
    static String inputLocation;
    static String outputLocation;
    static Long timeTaken;
    static Float Tps;
    static Integer returnCount;
    static long inputOffset = 0;
    static long outputOffset = 0;
    static String tag;
    static Integer fileCount = 0;
    static Integer headCount = 0;
    static String appdbName = null;
    static String auddbName = null;
    static Set<Book> errorFile = new HashSet<Book>();
    static Set<String> set = new HashSet<String>();
    static List<String> pattern = new ArrayList<String>();
    public static HashMap<String, HashMap<String, Book>> BookHashMap = new HashMap<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    static Clock offsetClock = Clock.offset(Clock.systemUTC(), Duration.ofHours(+7));
    static Instant start = Instant.now(offsetClock);
    static LocalDate currentdate = LocalDate.now();
    static Integer day = currentdate.getDayOfMonth();
    static Month month = currentdate.getMonth();
    static Integer year = currentdate.getYear();
    static List<String> ims_sources = new ArrayList<String>();
    static PropertiesReader propertiesReader = null;
    static ConnectionConfiguration connectionConfiguration = null;
    static Map<String, String> cdrImeiCheckMap = new HashMap<String, String>();
    static String procesStart_timeStamp = null;
    static Connection conn = null;
    static String attributeSplitor = null;
    static List<String> patternz = null;
    static StackTraceElement stackTrace = new Exception().getStackTrace()[0];

    public static void main111(String[] args) {
        File file = null;
        int insertedKey = 0;
        long startexecutionTime = new Date().getTime();
        try {
            sourceName = args[0];
            folderName = args[1];
            ApplicationContext context = SpringApplication.run(FileReaderHashApplicationWorking05012024.class, args);
            propertiesReader = (PropertiesReader) context.getBean("propertiesReader");
            connectionConfiguration = (ConnectionConfiguration) context.getBean("connectionConfiguration");
            conn = connectionConfiguration.getConnection();
            logger.info("Connection:" + conn);
            DateTimeFormatter tagDtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            LocalDateTime tagNow = LocalDateTime.now();
            tag = tagDtf.format(tagNow);
            procesStart_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            appdbName = propertiesReader.appdbName;
            auddbName = propertiesReader.auddbName;
            type = propertiesReader.typeOfProcess;
            value = propertiesReader.filesCount;  // FILES-COUNT-PER-REPORT=-1
            extension = propertiesReader.extension;
            sleep = propertiesReader.sleepTime;
            inputLocation = propertiesReader.inputLocation.replace("${DATA_HOME}", System.getenv("DATA_HOME"));
            outputLocation = propertiesReader.outputLocation.replace("${DATA_HOME}", System.getenv("DATA_HOME"));
            errorFlag = propertiesReader.errorReportFlag;
            returnCount = folderName.equalsIgnoreCase("all") ? propertiesReader.rowCountForSplit : 0;
            servername = propertiesReader.servername;
            ims_sources = propertiesReader.imsSources;
            attributeSplitor = folderName.equalsIgnoreCase("all") ? propertiesReader.commaDelimiter : propertiesReader.attributeSeperator;
            getCdrImeiLengthValues();
            long startexecutionTimeNew = new Date().getTime();
            //    long startexecutionTime = new Date().getTime();
            if (!folderName.contains("all")) {
                patternz = getFilePatternByOperatorSource();
            }
            if (!(ims_sources.contains(folderName))) { // "sm_ims".equals(folderName)
                set.addAll(propertiesReader.reportType);
                if (set.contains("null")) {
                    set = new HashSet<String>();
                }
            }
            if (value == -1) {
                File directory = new File(inputLocation + "/" + sourceName + "/" + folderName);
                value = directory.list().length;
                logger.info("Total File Count:" + value + " " + new File(directory, "null").exists());
                if (value == 0) {
                    logger.info("No file present");
                    String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                    insertReportv2("O", "0", Long.valueOf(0), Long.valueOf(0), Long.valueOf(0), Long.valueOf(0),
                            currentTime, currentTime, Float.valueOf(0), Float.valueOf(0), sourceName, folderName,
                            Long.valueOf(0), tag, 0, headCount);
                    insertedKey = insertModuleAudit(folderName.equalsIgnoreCase("all") ? "P2" : "P1", sourceName + "_" + folderName);
                    updateModuleAudit(200, "Success", "", insertedKey, new Date().getTime(), 0, 0);
                    System.exit(0);
                }
            }
            long filRetriver = 0;
            insertedKey = insertModuleAudit(folderName.equalsIgnoreCase("all") ? "P2" : "P1", sourceName + "_" + folderName);
            while (true) {
                startexecutionTimeNew = new Date().getTime();
                Instant startTimeOutput = Instant.now(offsetClock);
                String startTimeOutput1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                File folder = new File(inputLocation + "/" + sourceName + "/" + folderName);
                File[] listOfFiles = folder.listFiles();
                int filesLength = listOfFiles.length;
                if ((filesLength <= 0)) {  //&& (processed < value)
                    logger.info("No file present files Length " + filesLength + " Now processed- " + processed + "  and Value- " + value);
                    if (insertedKey != 0) {
                        updateModuleAudit(200, "Success", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                    }
                    System.exit(0);
                }
                filRetriver = filesLength > value ? value : filesLength;
                logger.info("Total Files Left " + filesLength + ", Files to be processed now " + filRetriver);
                for (int j = 0; j < filRetriver; j++) {
                    file = listOfFiles[j];
                    Instant startTime = Instant.now(offsetClock);
                    String startTime1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                    if (file.isFile() && !file.getName().endsWith(extension)) {
                        if ((!folderName.contains("all")) && (getEventTime(file.getName()) == null)) {
                            logger.debug("File Move to Error Folder: III FileName: " + file.getName() + ", Date: " + DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                                    + ", Start Time: " + startTime + ", End Time: " + Instant.now(offsetClock) + ", Time Taken: , Operator Name: " + sourceName + ", Source Name: " + folderName + ", TPS: " + Tps + ", Error: " + ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag + ", EventTime Tag  is null");
                            Path pathFolder = Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day);
                            if (!Files.exists(pathFolder)) {
                                Files.createDirectories(pathFolder);
                            }
                            Files.move(Paths.get(inputLocation + "/" + sourceName + "/" + folderName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/" + file.getName()));

                            insertReportv2("I", file.getName(), itotalCount, ierror, iduplicate, iinSet,
                                    startTime1.toString(), Instant.now(offsetClock).toString(), 0.0f, Tps, sourceName, folderName, inputOffset, tag, 1, headCount);
                            processed++;
                            continue;
                        }
                        logger.debug("Inside Loop::  Value: " + filRetriver + " . Processed : " + processed + " folder/sourceName" + folderName);
                        logger.debug("ims_sources:: " + ims_sources);
                        if (processed < filRetriver) {
                            fileName = file.getName();
                            if (ims_sources.contains(folderName)) { // "sm_ims".equals(folderName)
                                // for ims
                                boolean check = readBooksFromCSV_ims(file.getName());
                                if (!check) {
                                    processed++;
                                    move(fileName);
                                    continue;
                                }
                            } else {
                                // for others
                                boolean check = readBooksFromCSV(file.getName());
                                if (!check) {
                                    processed++;
                                    move(fileName);
                                    continue;
                                }
                            }

                            Path pathDay = Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/processed/" + year + "/" + month + "/" + day);
                            if (!Files.exists(pathDay)) {
                                Files.createDirectories(pathDay);
                                logger.info("Directory created");
                            }

                            // rename file
                            if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/processed/" + year + "/" + month + "/" + day + "/" + fileName))) {
                                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                                File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/processed/" + year + "/" + month + "/" + day
                                        + "/" + fileName);
                                String newName = fileName + "-" + sdf.format(timestamp);
                                File destFile = new File(outputLocation + "/" + sourceName + "/"
                                        + folderName + "/processed/" + year + "/" + month + "/" + day
                                        + "/" + newName);
                                if (sourceFile.renameTo(destFile)) {
                                    logger.info("File renamed successfully");
                                } else {
                                    logger.info("Failed to rename file");
                                }
                            }
                            // move file
                            Path temp = Files.move(Paths.get(inputLocation + "/" + sourceName + "/" + folderName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/processed/" + year + "/" + month + "/" + day + "/" + fileName));
                            if (temp != null) {
                                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                                LocalDateTime now = LocalDateTime.now();
                                Instant endTime = Instant.now(offsetClock);
                                timeTaken = Duration.between(startTime, endTime).toMillis();
                                Float timeTakenF = ((float) timeTaken / 1000);
                                if (timeTakenF == 0.0) {
                                    timeTakenF = (float) 0.001;
                                }
                                Tps = itotalCount / timeTakenF;
                                logger.debug(" Input File Report -- III FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTime + ", End Time: " + endTime + ", Time Taken: " + timeTakenF + ", Operator Name: " + sourceName + ", Source Name: " + folderName + ", TPS: " + Tps + ", Error: " + ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag);
                                fileCount++;
                                insertReportv2("I", fileName, itotalCount, ierror, iduplicate, iinSet,
                                        startTime1.toString(), endTime.toString(), timeTakenF, Tps,
                                        sourceName, folderName, inputOffset, tag, 1, headCount);
                                headCount = 0;
                                ierror = 0;
                                iinSet = 0;
                                itotalCount = 0;
                                iduplicate = 0;
                                inputOffset = 0;
                                //   logger.info("File moved successfully");
                                processed++;
                            } else {
                                logger.info("Failed to move the file");
                            }
                        } else {
                            logger.info("Output File Report Inside {if(notin logs)remove blck} :Value : " + filRetriver + "  Processed : " + processed);
                            makeCsv();
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                            LocalDateTime now = LocalDateTime.now();
                            Instant endTimeOutput = Instant.now(offsetClock);
                            timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                            Float timeTakenF = ((float) timeTaken / 1000);
                            if (timeTakenF == 0.0) {
                                timeTakenF = (float) 0.001;
                            }
                            Tps = totalCount / timeTakenF;
                            logger.info("Output File Report XXXX In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTimeOutput1 + ", End Time: " + endTimeOutput + ", Time Taken: " + timeTakenF + ", Operator Name: " + sourceName + ", Source Name: " + folderName + ", TPS: " + Tps + ", Error: " + error + ", inSet: " + inSet + ", totalCount: " + totalCount + ", duplicate: " + duplicate + ", volume: " + outputOffset + ", tag: " + tag);
                            insertReportv2("O", fileName, totalCount, error, duplicate, inSet,
                                    startTimeOutput1.toString(), endTimeOutput.toString(), timeTakenF,
                                    Tps, sourceName, folderName, outputOffset, tag, fileCount,
                                    headCount);
                            headCount = 0;
                            updateModuleAudit(202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                            error = 0;
                            inSet = 0;
                            totalCount = 0;
                            duplicate = 0;
                            outputOffset = 0;
                            fileCount = 0;
                            processed = 0;
                            BookHashMap.clear();
                            makeErrorCsv();
                            logger.info("Error Csv Created In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                            errorDuplicate = 0;
                            inErrorSet = 0;
                            errorFile.clear();
                        }
                    } else {   // file Extention Check
                        logger.info("No file or Incorrect file format present PATTERZN");
                        processed++;
                        continue;
                    }
                }
                logger.info("End Loop-- " + "Processed- : " + processed + "Value- : " + filRetriver);
                if (processed >= filRetriver) {  //processed <= value
                    logger.info("Final Processed is more than Retriver ");
                    makeCsv();
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    LocalDateTime now = LocalDateTime.now();
                    Instant endTimeOutput = Instant.now(offsetClock);
                    timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                    Float timeTakenF = ((float) timeTaken / 1000);
                    if (timeTakenF == 0.0) {
                        timeTakenF = (float) 0.001;
                    }
                    Tps = totalCount / timeTakenF;
                    logger.info("Output File Report Final Out FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: "
                            + startTimeOutput1 + ", End Time: " + endTimeOutput + ", Time Taken: " + timeTakenF
                            + ", Operator Name: " + sourceName + ", Source Name: " + folderName + ", TPS: " + Tps
                            + ", Error: " + error + ", inSet: " + inSet + ", totalCount: " + totalCount
                            + ", duplicate: " + duplicate + ", volume: " + outputOffset + ", tag: " + tag + "; File Processed  " + fileCount + "Total Final File count " + totalFileCount);
                    insertReportv2("O", fileName, totalCount, error, duplicate, inSet, startTimeOutput1.toString(),
                            endTimeOutput.toString(), timeTakenF, Tps, sourceName, folderName, outputOffset, tag,
                            fileCount, headCount);
                    totalFileCount += fileCount;
                    totalFileRecordsCount += totalCount;
                    updateModuleAudit(202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                    headCount = 0;
                    error = 0;
                    inSet = 0;
                    totalCount = 0;
                    duplicate = 0;
                    outputOffset = 0;
                    fileCount = 0;
                    processed = 0;
                    BookHashMap.clear();
                    makeErrorCsv();
                    logger.info("Error Csv Created Out FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                    errorDuplicate = 0;
                    inErrorSet = 0;
                    errorFile.clear();
                }
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.info("Alert " + file.getName() + " , Details : " + e.toString() + " || " + exceptionDetails + "");
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            updateModuleAudit(500, "Failure", e.getLocalizedMessage(), insertedKey, startexecutionTime, totalFileRecordsCount, totalFileCount);//numberOfRecord ,long totalFileCount
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("Not able to close the connection");
            }
        }
    }

    private static boolean readBooksFromCSV(String fileName) {
        logger.debug("inside readBooksFrom CSV with fileName " + fileName);
        Path pathToFile = Paths.get(inputLocation + "/" + sourceName + "/" + folderName + "/" + fileName);
        String line = null;
        try {
            logger.debug("File With Path  : " + pathToFile);
            eventTime = getEventTime(fileName);
            String folder_name = "";
            String file_name = "";
            String event_time = "";
            logger.debug("CDR_IMEI_LENGTH_VALUE " + cdrImeiCheckMap.get("CDR_IMEI_LENGTH_VALUE"));
            String[] myArray = cdrImeiCheckMap.get("CDR_IMEI_LENGTH_VALUE").split(",");
            BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
            if (folderName.equals("all")) {
                br.readLine();
                headCount++;
            }
            line = br.readLine();
            while (line != null) {
                String[] attributes = null;
                try {
                    inputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                    logger.debug("Actual LINE--: " + line);
                    attributes = line.split(attributeSplitor, -1);
                       if (folderName.equals("all")) {
                        folder_name = attributes[5];
                        file_name = attributes[6];
                        event_time = attributes[7];
                    } else {
                        folder_name = folderName;
                        file_name = fileName;
                        event_time = eventTime;
                    }
                    if (attributes[0].equalsIgnoreCase("IMEI")) {
                        headCount++;
                        line = br.readLine();
                        continue;
                    } else if (attributes[1].equals("") || attributes[2].equals("")
                            || ((cdrImeiCheckMap.get("CDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true"))
                            && !(Arrays.asList(myArray).contains(String.valueOf(attributes[0].length()))))) {
                        logger.debug(" Inside Wrong Record for CDR_IMEI_LENGTH_CHECK fails line imei : " + attributes[0]);
                        if ("1".equals(errorFlag)) {
                            Book bookError = createBook(attributes, folder_name, file_name, event_time);
                            if (errorFile.contains(bookError)) {
                                errorDuplicate++;
                            } else {
                                inErrorSet++;
                                errorFile.add(bookError);
                            }
                        }
                        line = br.readLine();
                        error++;
                        totalCount++;
                        ierror++;
                        itotalCount++;
                        continue;
                    }

                    if ((attributes[0].equals("") || attributes[0].matches("^[0]*$"))) {
                        logger.debug(" imei Value : " + attributes[0]);
                        if (cdrImeiCheckMap.get("CDR_NULL_IMEI_CHECK").equalsIgnoreCase("true")) {
                            logger.debug("Null Imei ,Check True, Error generator : " + attributes[0]);
                            if ("1".equals(errorFlag)) {
                                Book bookError = createBook(attributes, folder_name, file_name, event_time);
                                if (errorFile.contains(bookError)) {
                                    errorDuplicate++;
                                } else {
                                    inErrorSet++;
                                    errorFile.add(bookError);
                                }
                            }
                            line = br.readLine();
                            error++;
                            totalCount++;
                            ierror++;
                            itotalCount++;
                            continue;
                        } else {
                            attributes[0] = cdrImeiCheckMap.get("CDR_NULL_IMEI_REPLACE_PATTERN");
                            logger.debug("Null Imei ,Check False,Converted imei for futurise:" + attributes[0]);
                        }
                    }
                    if (!attributes[0].matches("^[ 0-9 ]+$")) {
                        logger.debug("Imei regex not match It is non numeric, Error generator  : " + attributes[0]);
                        if (cdrImeiCheckMap.get("CDR_ALPHANUMERIC_IMEI_CHECK").equalsIgnoreCase("true")) {
                            if ("1".equals(errorFlag)) {
                                Book bookError = createBook(attributes, folder_name, file_name, event_time);
                                if (errorFile.contains(bookError)) {
                                    errorDuplicate++;
                                } else {
                                    inErrorSet++;
                                    errorFile.add(bookError);
                                }
                            }
                            line = br.readLine();
                            error++;
                            totalCount++;
                            ierror++;
                            itotalCount++;
                            continue;
                        }
                    }

                    if (attributes[1].length() > 20 || attributes[2].length() > 20 || (!attributes[1].matches("^[a-zA-Z0-9_]*$")) || (!attributes[2].matches("^[a-zA-Z0-9_]*$"))) {
                        logger.debug(" Non alphaNumeric Or More than 20 not match for imsi -  : " + attributes[1] + " OR msisdn- " + attributes[2]);
                        if ("1".equals(errorFlag)) {
                            Book bookError = createBook(attributes, folder_name, file_name, event_time);
                            if (errorFile.contains(bookError)) {
                                errorDuplicate++;
                            } else {
                                inErrorSet++;
                                errorFile.add(bookError);
                            }
                        }
                        line = br.readLine();
                        error++;
                        totalCount++;
                        ierror++;
                        itotalCount++;
                        continue;
                    }
                    if (!set.isEmpty()) {
                        if (!set.contains(attributes[3])) {
                            line = br.readLine();
                            error++;
                            totalCount++;
                            ierror++;
                            itotalCount++;
                            continue;
                        }
                    }
                    Book book = createBook(attributes, folder_name, file_name, event_time);
                    if (BookHashMap.containsKey(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI())) {
                        // logger.info(" inside main Dpt");
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
                    totalCount++;
                    itotalCount++;
                } catch (Exception e) {
                    logger.error("Alert in  " + line + " ++ Alert " + e.toString() + " |||| " + e.getLocalizedMessage() + "");
                    raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + "; Wrong Cdr Record ", "<process_name>", "CDR_pre_processor"), 0);
                    if ("1".equals(errorFlag)) {
                        Book bookError = createBook(attributes, folder_name, file_name, event_time);
                        if (errorFile.contains(bookError)) {
                            errorDuplicate++;
                        } else {
                            inErrorSet++;
                            errorFile.add(bookError);
                        }
                    }
                    line = br.readLine();
                    error++;
                    totalCount++;
                    ierror++;
                    itotalCount++;
                    continue;

                }

            }
            br.close();
        } catch (Exception e) {
            logger.error("Alert in  " + line + "+Alert " + e.toString() + " || " + e.getLocalizedMessage() + "");
            raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + "; Wrong File Format ", "<process_name>", "CDR_pre_processor"), 0);
            return false;
        }

        return true;
    }

    private static boolean readBooksFromCSV_ims(String fileName) {
        logger.debug("inside readBooksFromCSV _ims with fileName " + fileName);
        eventTime = getEventTime(fileName);
        String imei = "";
        String imsi = "";
        String msisdn = "";
        String systemType = "";
        String recordType = "";
        Path pathToFile = Paths.get(inputLocation + "/" + sourceName + "/" + folderName + "/" + fileName);
        String line = null;
        try {
            String[] myArray = cdrImeiCheckMap.get("CDR_IMEI_LENGTH_VALUE").split(",");
            BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
            line = br.readLine();
            while (line != null) {
                inputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                String[] attributes = line.split(attributeSplitor, -1);
                //  logger.debug("LINE IMS--: " + attributes[0] + "," + attributes[1] + "," + attributes[2]); // 0-imei 1-imsi 2-mssidn
                if (attributes[0].equalsIgnoreCase("role-of-Node") || attributes[0].equalsIgnoreCase("role_of_Node")) {    //role_of_Node
                    headCount++;
                    line = br.readLine();
                    continue;
                }
                itotalCount++; // OCt
                totalCount++; // dec
                logger.debug("LINE IMS--:" + attributes[0] + "," + attributes[1] + "," + attributes[2] + "[iTotalCount:" + itotalCount + ";totalCount:" + totalCount + "]"); // 0-imei 1-imsi 2-mssidn
                if (attributes[1].equalsIgnoreCase("IMEI")) {
                    imei = attributes[2].replaceAll("-", "");  //.substring(0, 14)
                    String imsiTemp = attributes[3].toLowerCase();
                    if (imsiTemp.contains("imsi")) {
                        imsi = attributes[4];
                    }
                    if ("6".equals(attributes[9])) {
                        String tempMsisdn = attributes[10].replace("tel:+", "");
                        msisdn = tempMsisdn;
                    } else {
                        if ("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0])) {
                            String tempMsisdn = attributes[5].replace("tel:+", "");
                            msisdn = tempMsisdn;
                        } else if ("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0])) {
                            String tempMsisdn = attributes[6].replace("tel:+", "");
                            msisdn = tempMsisdn;
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
                    // totalCount++;
                    // itotalCount++;
                    Book book = createBookIms(imei, imsi, msisdn, systemType, recordType, folderName, fileName,
                            eventTime);
                    // error log

                    logger.debug("LENGTH--" + (((cdrImeiCheckMap.get("CDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true"))
                            && !(Arrays.asList(myArray).contains(String.valueOf(imei.length()))))));
                    logger.debug(!imei.matches("^[ 0-9 ]+$"));
                    if (imsi.equals("") || msisdn.equals("")
                            || ((cdrImeiCheckMap.get("CDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true"))
                            && !(Arrays.asList(myArray).contains(String.valueOf(imei.length()))))) {
                        logger.debug(" Inside Wrong Record line imei : " + imei);
                        // if (imei.equals("") || imsi.equals("") || msisdn.equals("")) { // commented
                        // to impl length null check
                        if ("1".equals(errorFlag)) {
                            Book bookError = createBookIms(imei, imsi, msisdn, systemType, recordType, folderName,
                                    fileName, eventTime);
                            if (errorFile.contains(bookError)) {
                                errorDuplicate++;
                            } else {
                                inErrorSet++;
                                errorFile.add(bookError);
                            }
                        }
                        line = br.readLine();
                        error++;
                        // totalCount++;
                        ierror++;
                        // itotalCount++;
                        continue;
                    }

                    if (imsi.length() > 20 || msisdn.length() > 20 || (!imsi.matches("^[a-zA-Z0-9_]*$")) || (!msisdn.matches("^[a-zA-Z0-9_]*$"))) {
                        logger.info(" Non alphaNumeric Or More than 20 not match for imsi -  : " + imsi + " OR msisdn- " + msisdn);
                        if ("1".equals(errorFlag)) {
                            Book bookError = createBookIms(imei, imsi, msisdn, systemType, recordType, folderName,
                                    fileName, eventTime);
                            if (errorFile.contains(bookError)) {
                                errorDuplicate++;
                            } else {
                                inErrorSet++;
                                errorFile.add(bookError);
                            }
                        }
                        line = br.readLine();
                        error++;
                        ierror++;
                        continue;
                    }

                    if ((imei.equals("") || imei.matches("^[0]*$"))) {
                        logger.debug("Null imei  : " + imei);
                        if (cdrImeiCheckMap.get("CDR_NULL_IMEI_CHECK").equalsIgnoreCase("true")) {
                            logger.debug("Null Imei ,Check True, Error generator : " + imei);
                            if ("1".equals(errorFlag)) {
                                Book bookError = createBookIms(imei, imsi, msisdn, systemType, recordType, folderName,
                                        fileName, eventTime);
                                if (errorFile.contains(bookError)) {
                                    errorDuplicate++;
                                } else {
                                    inErrorSet++;
                                    errorFile.add(bookError);
                                }
                            }
                            line = br.readLine();
                            error++;
                            ierror++;
                            continue;
                        } else {
                            attributes[0] = cdrImeiCheckMap.get("CDR_NULL_IMEI_REPLACE_PATTERN");
                            logger.debug("Null Imei ,Check False,Converted imei for futurise:" + attributes[0]);
                        }
                    }

                    if (!imei.matches("^[ 0-9 ]+$")) {
                        logger.info("AlphaNumeric  imei  : " + imei);
                        if (cdrImeiCheckMap.get("CDR_ALPHANUMERIC_IMEI_CHECK").equalsIgnoreCase("true")) {
                            logger.debug("AlphaNumeric ,Check True, Error generator : " + imei);
                            if ("1".equals(errorFlag)) {
                                Book bookError = createBookIms(imei, imsi, msisdn, systemType, recordType, folderName,
                                        fileName, eventTime);
                                if (errorFile.contains(bookError)) {
                                    errorDuplicate++;
                                } else {
                                    inErrorSet++;
                                    errorFile.add(bookError);
                                }
                            }
                            line = br.readLine();
                            error++;
                            ierror++;
                            continue;
                        }
                    }

                    if (BookHashMap.containsKey(book.getIMEI())) {
                        if (!BookHashMap.get(book.getIMEI()).containsKey(book.getMSISDN())) {
                            BookHashMap.get(book.getIMEI()).put(book.getMSISDN(), book);
                            inSet++;
                            iinSet++;
                            outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line
                            // separator
                        } else {
                            duplicate++;
                            iduplicate++;
                        }
                    } else {
                        HashMap<String, Book> bookMap = new HashMap<>();
                        bookMap.put(book.getMSISDN(), book);
                        BookHashMap.put(book.getIMEI(), bookMap);
                        inSet++;
                        iinSet++;
                        outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                    }
                    line = br.readLine();
                    // continue;
                } else {
                    line = br.readLine();
                    error++;
                    ierror++;
                    // totalCount++;
                    // itotalCount++;
                }
            }
            br.close();
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.error("In line -" + line + " !!!! Alert " + e.toString() + " || " + exceptionDetails);
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString() + "; Wrong File Format");
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            //////
            return false;
        }
        return true;
    }

    public static void makeCsv() {
        FileWriter fileWriter = null;
        Integer i = 1;
        try {
            Path pathFolder = Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/");
            if (!Files.exists(pathFolder)) {
                Files.createDirectories(pathFolder);
                logger.info("Directory created");
            }
            // rename file
            if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName))) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                String newName = fileName + "-" + sdf.format(timestamp);
                File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + newName);
                if (sourceFile.renameTo(destFile)) {
                    logger.info("File renamed successfully");
                } else {
                    logger.info("Failed to rename file");
                }
            }
            if (returnCount == 0) {
                logger.info("inside non split block");
                fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);
                for (HashMap.Entry<String, HashMap<String, Book>> csvf
                        : BookHashMap.entrySet()) {
                    // String levelOne = csvf.getKey();
                    for (HashMap.Entry<String, Book> csvf3
                            : csvf.getValue().entrySet()) {
                        // String levelTwo = csvf2.getKey();
                        fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getRecordType()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getSystemType()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));
                        fileWriter.append(propertiesReader.newLineSeprator);
                        fileWriter.flush();
                    }
                }
            } else {
                logger.info("inside split block");
                Integer count = 0;
                fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);

//                List<String> list = new ArrayList<>(BookHashMap.keySet()); // to reduce the same imei simultanously
//                Collections.shuffle(list);
//                Map<String, HashMap<String, Book>> NewBookHashMap = new HashMap<>();
//                list.forEach(k -> NewBookHashMap.put(k, BookHashMap.get(k)));
                for (HashMap.Entry<String, HashMap<String, Book>> csvf
                        : BookHashMap.entrySet()) {
                    // String levelOne = csvf.getKey();
                    for (HashMap.Entry<String, Book> csvf3
                            : csvf.getValue().entrySet()) {
                        // String levelTwo = csvf2.getKey();
                        if (count < returnCount) {
                            // logger.info("count less than return count: " + count);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getRecordType()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSystemType()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));
                            fileWriter.append(propertiesReader.newLineSeprator);
                            // logger.info("Entry " + count + ": " + fileWriter + ": imei:" +
                            // String.valueOf(csvf3.getValue().getIMEI())
                            // + ", imsi:" + String.valueOf(csvf3.getValue().getIMSI())
                            // + ", msisdn:" + String.valueOf(csvf3.getValue().getMSISDN())
                            // + ", recordtype:" + String.valueOf(csvf3.getValue().getRecordType())
                            // + ", systemtype:" + String.valueOf(csvf3.getValue().getSystemType())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getSourceName())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getFileName())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getEventTime()));

                            count++;
                            fileWriter.flush();
                        } else {
                            // logger.info("count greater than split count: " + count);
                            if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName))) {
                                File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                                String newName = fileName + "_00" + i;
                                i++;
                                File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + newName);
                                if (sourceFile.renameTo(destFile)) {
                                    logger.info("File split successfully: " + newName);
                                } else {
                                    logger.info("Failed to split file");
                                }
                            }
                            count = 0;
                            fileWriter = new FileWriter(
                                    outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                            fileWriter.append(propertiesReader.fileHeader);
                            fileWriter.append(propertiesReader.newLineSeprator);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getRecordType()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSystemType()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));
                            fileWriter.append(propertiesReader.newLineSeprator);
                            // logger.info("Entry " + count + ": " + fileWriter + ": imei:" +
                            // String.valueOf(csvf3.getValue().getIMEI())
                            // + ", imsi:" + String.valueOf(csvf3.getValue().getIMSI())
                            // + ", msisdn:" + String.valueOf(csvf3.getValue().getMSISDN())
                            // + ", recordtype:" + String.valueOf(csvf3.getValue().getRecordType())
                            // + ", systemtype:" + String.valueOf(csvf3.getValue().getSystemType())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getSourceName())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getFileName())
                            // + ", sourceName:" + String.valueOf(csvf3.getValue().getEventTime()));

                            count++;
                            fileWriter.flush();
                        }
                    }
                }
                if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName))) {
                    File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                    String newName = fileName + "_00" + i;
                    i++;
                    File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + newName);
                    if (sourceFile.renameTo(destFile)) {
                        logger.info("File split successfully: " + newName);
                    } else {
                        logger.info("Failed to split file");
                    }
                }
            }
            logger.info("CSV file was created successfully !!!");
        } catch (Exception e) {
            logger.info("Error in CsvFileWriter !!!");
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.info("Alert " + e.toString() + " || " + exceptionDetails);
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
        } finally {
            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                logger.info("Error while flushing/closing fileWriter !!!");
                Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
                placeholderMapForAlert.put("<e>", e.toString());
                placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
                raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
                logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            }
        }
    }

    public static void makeErrorCsv() {
        FileWriter fileWriter = null;
        try {
            Path pathDay = Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day);
            if (!Files.exists(pathDay)) {
                Files.createDirectories(pathDay);
                logger.info("Directory created for error");
            }
            // rename file
            if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/" + fileName))) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/" + fileName);
                String newName = fileName + "-" + sdf.format(timestamp);
                File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/" + newName);
                if (sourceFile.renameTo(destFile)) {
                    logger.info("File renamed successfully");
                } else {
                    logger.info("Failed to rename file");
                }
            }

            if (!errorFile.isEmpty()) {// optimise to not create folder
                fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/" + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);
            }
            for (Book csvf : errorFile) {
                fileWriter.append(String.valueOf(csvf.getIMEI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getIMSI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getMSISDN()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getRecordType()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getSystemType()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getSourceName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getFileName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getEventTime()));
                fileWriter.append(propertiesReader.newLineSeprator);
                fileWriter.flush();
            }
            logger.info("CSV file was created successfully for Error File!!!");
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.info("Alert " + e.toString() + " || " + exceptionDetails);
            logger.info("Error in CsvFileWriter for Error File!!!");
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.flush();
                    fileWriter.close();
                }
            } catch (IOException e) {
                logger.info("Error while flushing/closing fileWriter for Error File!!!");
                Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
                placeholderMapForAlert.put("<e>", e.toString());
                placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
                raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
                logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            }
        }
    }

    private static Book createBook(String[] metadata, String source_name, String file_name, String event_time) {
        // String imei = metadata[0].substring(0, 1 4);
        String imei = metadata[0];
        String imsi = metadata[1];
        String msisdn = ((metadata[2].trim().startsWith("19") || metadata[2].trim().startsWith("00"))
                ? metadata[2].substring(2)
                : metadata[2]).replace("1AO", "855");
        String recordType = metadata[3] == null ? "" : metadata[3];
        String systemType = metadata[4] == null ? "" : metadata[4];
        String sourceName = source_name;
        String fileName = file_name;
        String eventTime = event_time;
        return new Book(imei, imsi, msisdn, recordType, systemType, sourceName, fileName, eventTime);
    }

    private static Book createBookIms(String IMEI, String IMSI, String MSISDN, String system_type, String record_type,
                                      String source_name, String file_name, String event_time) {
        String imei = IMEI;
        String imsi = IMSI;
        String msisdn = MSISDN;
        String recordType = system_type;
        String systemType = record_type;
        String sourceName = source_name;
        String fileName = file_name;
        String eventTime = event_time;
        return new Book(imei, imsi, msisdn, recordType, systemType, sourceName, fileName, eventTime);
    }

    public static String getEventTime(String fileName) {
        String date = "";
        if (!folderName.contains("all")) {
            pattern.addAll(patternz);
            if (pattern.contains("null")) {
                pattern = new ArrayList<String>();
            }
        }
        for (String filePattern : pattern) {
            String[] attributes = filePattern.split("-", -1);
            if (fileName.contains(attributes[0])) {
                date = fileName.substring(fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1]),
                        fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1])
                                + Integer.parseInt(attributes[2]));
            }
        }
        String imei_arrivalTime = null;
        try {
            String dateType = "yyyyMMdd";
            if (propertiesReader.ddMMyyyySource.contains(folderName)) {
                dateType = "ddMMyyyy";
            } else if (propertiesReader.yyMMddSource.contains(folderName)) {
                dateType = "yyMMdd";
            } else if (propertiesReader.ddMMyySource.contains(folderName)) {
                dateType = "ddMMddyy";
            }
            imei_arrivalTime = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat(dateType).parse(date));
        } catch (Exception e) {
            logger.info(fileName + " Unable to parse Date ,Defined Pattern" + patternz + ", Error " + e);
        }
        return imei_arrivalTime;
    }

    public static void insertReportv2(String fileType, String fileName, Long totalRecords, Long totalErrorRecords,
                                      Long totalDuplicateRecords, Long totalOutputRecords, String startTime, String endTime, Float timeTaken,
                                      Float tps, String operatorName, String sourceName, long volume, String tag, Integer FileCount,
                                      Integer headCount) {
        try (Statement stmt = conn.createStatement();) {
            endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            if (fileType.equalsIgnoreCase("O")) {
                headCount = headCount + 1;
            }
            String dateFunction = defaultDateNow(conn.toString().contains("oracle"));

            String dateFunc = defaultStringtoDate(procesStart_timeStamp);

            String sql = "Insert into " + appdbName
                    + ".cdr_file_pre_processing_detail(CREATED_ON,MODIFIED_ON,FILE_TYPE,TOTAL_RECORDS,TOTAL_ERROR_RECORDS,TOTAL_DUPLICATE_RECORDS,TOTAL_OUTPUT_RECORDS,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername )"
                    + "values(" + dateFunc + " , " + dateFunction + " , '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
                    + totalDuplicateRecords + "," + totalOutputRecords + ",'" + fileName + "'," + defaultStringtoDate(startTime) + ","
                    + defaultStringtoDate(endTime) + "," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "'    )";

            //  Working with mysql
//            String sql = "Insert into " + appdbName
//                    + ".cdr_file_pre_processing_detail(CREATED_ON,MODIFIED_ON,FILE_TYPE,TOTAL_RECORDS,TOTAL_ERROR_RECORDS,TOTAL_DUPLICATE_RECORDS,TOTAL_OUTPUT_RECORDS,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername )"
//                    + "values(" + dateFunc + " , " + dateFunction + " , '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
//                    + totalDuplicateRecords + "," + totalOutputRecords + ",'" + fileName + "','" + startTime + "','"
//                    + endTime + "'," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
//                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "'    )";
            logger.info("Inserting into table  cdr _pre_processing  _report:: " + sql);
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", "not able to insert in cdr_file_pre_processing_detail," + e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            //      System.exit(0); 
        }
    }

    public static void getCdrImeiLengthValues() {
        try (Statement stmt = conn.createStatement();) {
            String sql = "select tag , value  from " + appdbName + ".sys_param where tag in   ('CDR_IMEI_LENGTH_CHECK' ,'CDR_IMEI_LENGTH_VALUE','CDR_NULL_IMEI_CHECK','CDR_NULL_IMEI_REPLACE_PATTERN'   , 'CDR_ALPHANUMERIC_IMEI_CHECK')";
            logger.info("Fetching details  " + sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                cdrImeiCheckMap.put(rs.getString("tag"), rs.getString("value"));
            }
        } catch (Exception e) {
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            System.exit(0);
        }

    }

    public static int insertModuleAudit(String featureName, String processName) {
        int generatedKey = 0;
        String query = " insert into  " + auddbName + ".modules_audit_trail " + "(status_code,status,feature_name,"
                + "info, count2,action," + "server_name,execution_time,module_name,failure_count) "
                + "values('201','Initial', '" + featureName + "', '" + processName + "' ,'0','Insert', '"
                + servername + "','0','ETL','0')";
        logger.info(query);
        try {
            PreparedStatement ps = null;
            if (conn.toString().contains("oracle")) {
                ps = conn.prepareStatement(query, new String[]{"ID"});
            } else {
                ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            }
            logger.debug("Going to execute  ");
            ps.execute();
            logger.debug("Going for getGenerated key  ");
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                generatedKey = rs.getInt(1);
            }
            logger.info("Inserted record's ID: " + generatedKey);
            rs.close();
        } catch (Exception e) {
            logger.error("Failed  " + e);

        }
        return generatedKey;
    }

    public static void updateModuleAudit(int statusCode, String status, String errorMessage, int id, long executionStartTime, long numberOfRecord, long totalFileCount) {
        // String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);

        // for Oracle
        //   String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);
        // for Mysql execution_time = TIMEDIFF(now() ,created_on)

        String exec_time = " TIMEDIFF(now() ,created_on) ";
        if (conn.toString().contains("oracle")) {
            long milliseconds = (new Date().getTime()) - executionStartTime;
            String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);
            exec_time = " '" + executionFinishTiime + "' ";
        }

        try (Statement stmt = conn.createStatement()) {
            String query = "update   " + auddbName + ".modules_audit_trail set status_code='" + statusCode + "',status='" + status + "',error_message='" + errorMessage + "', count='" + numberOfRecord + "',"
                    + "action='insert', execution_time = " + exec_time + "  ,  modified_on = " + defaultDateNow() + " , failure_count='0' , count2='" + totalFileCount + "'     where  id = " + id;
            logger.info(query);
            stmt.executeUpdate(query);
        } catch (Exception e) {
            logger.error("Failed  " + e);
        }
    }

    private static List<String> getFilePatternByOperatorSource() {
        try (Statement stmt = conn.createStatement();) {
            String sql = "select  value  from " + appdbName + ".sys_param where tag =  '" + sourceName.toUpperCase() + "_" + folderName.toUpperCase() + "_FILE_PATTERN'   ";
            logger.info("Fetching details for FILE_PATTERN " + sql);
            ResultSet rs = stmt.executeQuery(sql);
            String response = "null";
            while (rs.next()) {
                response = rs.getString("value");
            }
            logger.info("Fetching response  " + response);
            return Arrays.asList(response.split(","));
        } catch (Exception e) {
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            System.exit(0);
        }
        return null;
    }

    public static String getAlertbyId(String alertId) {
        String description = "";
        try (Statement stmt = conn.createStatement();) {
            String sql = "select description from " + appdbName + ".cfg_feature_alert where alert_id='" + alertId + "'";
            logger.info("Fetching alert message by alert id from alertDb " + sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                description = rs.getString("description");
            }
        } catch (Exception e) {
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "CDR_pre_processor");
            raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.info("Alert [ALERT_006] is raised. So, doing nothing.");
            System.exit(0);
        }
        return description;
    }

    public static void raiseAlert(String alertId, Map<String, String> bodyPlaceHolderMap, Integer userId) {
        try (Statement stmt = conn.createStatement();) {
            String alertDescription = getAlertbyId(alertId);
            if (Objects.nonNull(bodyPlaceHolderMap)) {
                for (Map.Entry<String, String> entry
                        : bodyPlaceHolderMap.entrySet()) {
                    logger.info("Placeholder key : " + entry.getKey() + " value : " + entry.getValue());
                    alertDescription = alertDescription.replaceAll(entry.getKey(), entry.getValue());
                }
            }
            logger.info("alert message: " + alertDescription);
            String sql = "Insert into " + appdbName + ".sys_generated_alert (alert_id,created_on,modified_on,description,status,user_id)" + "values('"
                    + alertId + "', " + defaultDateNow() + " ," + defaultDateNow() + " ,'" + alertDescription + "',0," + userId + ")";
            logger.info("Inserting alert into running alert db" + sql);
            stmt.executeUpdate(sql);
            // Sy stem.exi t(0);
        } catch (Exception e) {
            logger.error("Error in raising Alert. So, doing nothing." + e);
            //  Sys tem.ex it(0);
        }
    }

    public static String defaultStringtoDate(String date1) {
        if (conn.toString().contains("oracle")) {
            return "to_timestamp('" + date1 + "','YYYY-MM-DD HH24:MI:SS')";
        } else {
            return "'" + date1 + "'";
        }
    }

    public static String defaultDateNow(boolean isOracle) {
        if (isOracle) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')"; // commented by sharad
            return date;
        } else {
            return "now()";
        }
    }

    public static String defaultDateNow() {
        if (conn.toString().contains("oracle")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            String date = "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')"; // commented by sharad
            return date;
        } else {
            return "now()";
        }
    }

    public static void move(String fileName) throws IOException {
        Path pathFile = Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/errorFile");
        if (!Files.exists(pathFile)) {
            Files.createDirectories(pathFile);
            logger.info("Directory created");
        }
        // rename file
        if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + fileName))) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + fileName);
            String newName = fileName + "-" + sdf.format(timestamp);
            File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + newName);
            if (sourceFile.renameTo(destFile)) {
                logger.info("File renamed successfully");
            } else {
                logger.info("Failed to rename file");
            }
        }
        // move file
        Path temp = null;
        try {
            temp = Files.move(Paths.get(inputLocation + "/" + sourceName + "/" + folderName + "/" + fileName), Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/error/" + year + "/" + month + "/"
                    + day + "/errorFile/" + fileName));
        } catch (Exception e) {
            logger.warn(" File   " + fileName + " Not able to move ");
        }
        if (temp != null) {
            logger.info("File moved in Error Folder successfully");
        } else {
            logger.warn("Failed to move the file in Error Folder" + fileName);
        }
    }

}
