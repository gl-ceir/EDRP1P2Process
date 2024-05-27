package com.gl.reader.service;

import com.gl.reader.configuration.ConnectionConfiguration;
import com.gl.reader.configuration.PropertiesReader;
import com.gl.reader.constants.Alerts;
import com.gl.reader.dto.FilePreProcessing;
import com.gl.reader.dto.ModulesAudit;
import com.gl.reader.model.Book;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.gl.reader.dto.Alert.raiseAlert;
import static com.gl.reader.dto.ModulesAudit.updateModuleAudit;
import static com.gl.reader.dto.SysParam.getFilePatternByOperatorSource;
import static com.gl.reader.dto.SysParam.imeiLengthValueCheck;
import static com.gl.reader.service.impl.CsvCreater.*;
import static com.gl.reader.service.impl.FileReaderService.getArrivalTimeFromFilePattern;
import static com.gl.reader.service.impl.FileReaderService.moveFileToError;
import static com.gl.reader.service.impl.RecordServiceImpl.readRecordsFromFileAndCreateHash;


@Component
public class ProcessController {

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    public static Logger logger = LogManager.getLogger(ProcessController.class);
    public static long duplicate = 0;
    public static long error = 0;
    public static long blacklisterror = 0;
    public static long inSet = 0;
    public static long totalCount = 0;
    public static long iduplicate = 0;
    public static long ierror = 0;
    public static long iBlackListerror = 0;
    public static long iinSet = 0;
    public static long itotalCount = 0;
    public static long value;
    public static long processed = 0;
    public static String fileName;
    public static String extension;
    public static String servername;
    public static String sourceName;
    public static String operatorName;
    public static String eventTime;
    public static long errorDuplicate = 0;
    public static long errorBlacklistDuplicate = 0;
    public static long inErrorSet = 0;
    public static long inBlacklistErrorSet = 0;

    public static long totalFileCount = 0;
    public static long totalFileRecordsCount = 0;
    public static String inputLocation;
    public static String outputLocation;
    public static Long timeTaken;
    public static Float Tps;
    public static Integer returnCount;
    public static long inputOffset = 0;
    public static long outputOffset = 0;
    public static String tag;
    public static Integer fileCount = 0;
    public static Integer headCount = 0;
    public static String appdbName = null;
    public static String edrappdbName = null;

    public static String auddbName = null;
    public static Set<Book> errorFile = new HashSet<>();
    public static Set<Book> errorBlacklistFile = new HashSet<>();
    public static Set<String> reportTypeSet = new HashSet<>();
    public static List<String> pattern = new ArrayList<>();
    public static HashMap<String, HashMap<String, Book>> BookHashMap = new HashMap<>();
    public static Clock offsetClock = Clock.offset(Clock.systemUTC(), Duration.ofHours(+7));
    public static LocalDate currentdate = LocalDate.now();
    public static Integer day = currentdate.getDayOfMonth();
    public static Month month = currentdate.getMonth();
    public static Integer year = currentdate.getYear();
    public static List<String> ims_sources = new ArrayList<String>();
    public static PropertiesReader propertiesReader = null;
    public static Map<String, String> imeiValCheckMap = new HashMap<String, String>();
    public static String procesStart_timeStamp = null;
    public static Connection conn = null;
    public static String attributeSplitor = null;
    public static List<String> file_patterns = null;
    public static ConnectionConfiguration connectionConfiguration = null;

    public static void startApplication(ApplicationContext context, String[] args) {
        File file = null;
        int insertedKey = 0;
        long startexecutionTime = new Date().getTime();
        try {
            operatorName = args[0];
            sourceName = args[1];

            connectionConfiguration = (ConnectionConfiguration) context.getBean("connectionConfiguration");
            conn = connectionConfiguration.getConnection();
            logger.info("Connection::::::::::" + conn);
            DateTimeFormatter tagDtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            tag = tagDtf.format(LocalDateTime.now());
            propertiesReader = (PropertiesReader) context.getBean("propertiesReader");
            procesStart_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            appdbName = propertiesReader.appdbName;
            edrappdbName = propertiesReader.edrappdbName;
            auddbName = propertiesReader.auddbName;
            value = propertiesReader.filesCount;  // FILES-COUNT-PER-REPORT=-1
            extension = propertiesReader.extension;
            inputLocation = propertiesReader.inputLocation.replace("{DATA_HOME}", System.getenv("DATA_HOME"));   // System.getenv("DATA_HOME")
            outputLocation = propertiesReader.outputLocation.replace("{DATA_HOME}", System.getenv("DATA_HOME"));  //System.getenv("DATA_HOME")
            returnCount = sourceName.equalsIgnoreCase("all") ? propertiesReader.rowCountForSplit : 0;
            servername = propertiesReader.servername;
            ims_sources = propertiesReader.imsSources;
            attributeSplitor = sourceName.equalsIgnoreCase("all") ? propertiesReader.commaDelimiter : propertiesReader.attributeSeperator;
            imeiValCheckMap = imeiLengthValueCheck(conn);
            long startexecutionTimeNew = new Date().getTime();
            if (!sourceName.contains("all")) {
                file_patterns = getFilePatternByOperatorSource(conn, operatorName, sourceName);
            }
            if (!(ims_sources.contains(sourceName))) { // "sm_ims".equals(folderName)
                reportTypeSet.addAll(propertiesReader.reportType);
                if (reportTypeSet.contains("null")) {
                    reportTypeSet = new HashSet<>();
                }
            }

            checkFilePresence();

            long filRetriver = 0;
            insertedKey = ModulesAudit.insertModuleAudit(conn, sourceName.equalsIgnoreCase("all") ? "P2" : "P1", operatorName + "_" + sourceName, servername);
            while (true) {
                startexecutionTimeNew = new Date().getTime();
                Instant startTimeOutput = Instant.now(offsetClock);
                String startTimeOutput1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                File folder = new File(inputLocation + "/" + operatorName + "/" + sourceName);
                File[] listOfFiles = folder.listFiles();
                int filesLength = listOfFiles.length;
                if ((filesLength <= 0)) {  //&& (processed < value)
                    logger.info("No file present files Length " + filesLength + " Now processed- " + processed + "  and Value- " + value);
                    if (insertedKey != 0) {
                        updateModuleAudit(conn, 200, "Success", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                    }
                    System.exit(0);
                }
                filRetriver = filesLength > value ? value : filesLength;
                logger.info("Total Files Left " + filesLength + ", Files to be processed now " + filRetriver);
                for (int j = 0; j < filRetriver; j++) {
                    file = listOfFiles[j];
                    Instant startTime = Instant.now(offsetClock);
                    String startTime1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                    if (file.isFile() && !file.getName().endsWith(extension)) {
                        eventTime = getArrivalTimeFromFilePattern(sourceName, file.getName());
                        if ((!sourceName.contains("all")) && (eventTime == null)) {
                            logger.info("File Move to Error Folder: III FileName: " + file.getName() + ", Date: " + DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                                    + ", Start Time: " + startTime + ", End Time: " + Instant.now(offsetClock) + ", Time Taken: , Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error::: "
                                  +",iBlackListerror:" + iBlackListerror +  ",,i error:"+ ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag + ", EventTime Tag  is null");
                            Path pathFolder = Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day);
                            if (!Files.exists(pathFolder)) {
                                Files.createDirectories(pathFolder);
                            }
                            Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/" + file.getName()));
                            FilePreProcessing.insertReportv2("I", file.getName(), itotalCount, ierror, iduplicate, iinSet,
                                    startTime1.toString(), Instant.now(offsetClock).toString(), 0.0f, Tps, operatorName, sourceName, inputOffset, tag, 1, headCount, servername  ,iBlackListerror);
                            processed++;
                            continue;
                        }
                        logger.info("Inside Loop::  Value: " + filRetriver + " . Processed : " + processed + " folder/sourceName" + sourceName);
                        if (processed < filRetriver) {
                            fileName = file.getName();
                            boolean check = readRecordsFromFileAndCreateHash(file.getName());
                            if (!check) {
                                processed++;
                                moveFileToError(fileName);
                                continue;
                            }
                            createAndRenameFileIfExists(outputLocation + "/" + operatorName + "/" + sourceName + "/processed/" + year + "/" + month + "/" + day, fileName);
                            // move file
                            Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/processed/" + year + "/" + month + "/" + day + "/" + fileName));
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                            LocalDateTime now = LocalDateTime.now();
                            Instant endTime = Instant.now(offsetClock);
                            timeTaken = Duration.between(startTime, endTime).toMillis();
                            float timeTakenF = ((float) timeTaken / 1000);
                            if (timeTakenF == 0.0) {
                                timeTakenF = (float) 0.001;
                            }
                            Tps = itotalCount / timeTakenF;
                            logger.info(" Input File Report -- III FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTime + ", End Time: " + endTime + ", Time Taken: " + timeTakenF + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error: " + ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag);
                            fileCount++;
                            FilePreProcessing.insertReportv2("I", fileName, itotalCount, ierror, iduplicate, iinSet, startTime1.toString(), endTime.toString(), timeTakenF, Tps, operatorName, sourceName, inputOffset, tag, 1, headCount, servername,iBlackListerror);
                            headCount = 0;
                            ierror = 0;
                            iBlackListerror=0;
                            iinSet = 0;
                            itotalCount = 0;
                            iduplicate = 0;
                            inputOffset = 0;
                            logger.info("File moved successfully and data inserted");
                            processed++;
                        } else {
                            logger.info("Output File Report Inside {CHEC IF it is working ?? } :Value : " + filRetriver + "  Processed : " + processed);
                            makeCsv(outputLocation, operatorName, sourceName, fileName, returnCount);
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                            LocalDateTime now = LocalDateTime.now();
                            Instant endTimeOutput = Instant.now(offsetClock);
                            timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                            float timeTakenF = ((float) timeTaken / 1000);
                            if (timeTakenF == 0.0) {
                                timeTakenF = (float) 0.001;
                            }
                            Tps = totalCount / timeTakenF;
                            logger.info("Output File Report  In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTimeOutput1 + ", End Time: " + endTimeOutput + ", Time Taken: " + timeTakenF + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error: " + error + ", inSet: " + inSet + ", totalCount: " + totalCount + ", duplicate: " + duplicate + ", volume: " + outputOffset + ", tag: " + tag);
                            FilePreProcessing.insertReportv2("O", fileName, totalCount, error, duplicate, inSet, startTimeOutput1, endTimeOutput.toString(), timeTakenF, Tps, operatorName, sourceName, outputOffset, tag, fileCount, headCount, servername ,blacklisterror);
                            headCount = 0;
                            updateModuleAudit(conn, 202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                            error = 0;
                            blacklisterror=0;
                            inSet = 0;
                            totalCount = 0;
                            duplicate = 0;
                            outputOffset = 0;
                            fileCount = 0;
                            processed = 0;
                            BookHashMap.clear();
                            makeErrorCsv(outputLocation, operatorName, sourceName, fileName, errorFile);//makeErrorCsv();
                            logger.info("Error Csv Created In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                            errorDuplicate = 0;
                            inErrorSet = 0;
                            errorFile.clear();
                            makeBlacklistErrorCsv(outputLocation, operatorName, sourceName, fileName, errorBlacklistFile);//makeErrorCsv();
                            logger.info(" Blacklist Error Csv Craeted" + ", Error: " + errorBlacklistDuplicate + ", inBlacklistErrorSet: " + inBlacklistErrorSet);
                            errorBlacklistDuplicate = 0;

                        }
                    } else {   // file Extention Check
                        logger.info("No file or Incorrect file format present PATTERZN");
                        processed++;
                        continue;
                    }
                }
                logger.info("End Loop-- " + "Processed- : " + processed + "Value- : " + filRetriver);
                if (processed >= filRetriver) {  //processed <= value
                    logger.info("Final Processed is more than Retriver  !!!CHECKED its working");
                    makeCsv(outputLocation, operatorName, sourceName, fileName, returnCount);
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    LocalDateTime now = LocalDateTime.now();
                    Instant endTimeOutput = Instant.now(offsetClock);
                    timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                    float timeTakenF = ((float) timeTaken / 1000);
                    if (timeTakenF == 0.0) {
                        timeTakenF = (float) 0.001;
                    }
                    Tps = totalCount / timeTakenF;
                    FilePreProcessing.insertReportv2("O", fileName, totalCount, error, duplicate, inSet, startTimeOutput1, endTimeOutput.toString(), timeTakenF, Tps, operatorName, sourceName, outputOffset, tag, fileCount, headCount, servername,blacklisterror);
                    totalFileCount += fileCount;
                    totalFileRecordsCount += totalCount;
                    updateModuleAudit(conn, 202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                    headCount = 0;
                    error = 0;
                    blacklisterror=0;
                    inSet = 0;
                    totalCount = 0;
                    duplicate = 0;
                    outputOffset = 0;
                    fileCount = 0;
                    processed = 0;
                    BookHashMap.clear();
                    makeErrorCsv(outputLocation, operatorName, sourceName, fileName, errorFile);//makeErrorCsv();//
                    logger.info("Error Csv Created Out FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                    errorDuplicate = 0;
                    inErrorSet = 0;
                    errorFile.clear();

                    makeBlacklistErrorCsv(outputLocation, operatorName, sourceName, fileName, errorBlacklistFile);//makeErrorCsv();
                    logger.info(" Blacklist Error Csv Craeted" + ", Error: " + errorBlacklistDuplicate + ", inBlacklistErrorSet: " + inBlacklistErrorSet);
                    errorBlacklistDuplicate = 0;
                    inBlacklistErrorSet=0;

                }
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ProcessController.class.getName())).collect(Collectors.toList()).get(0) + "]");
            raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + ". in file  ", "<process_name>", "EDR_pre_processor"), 0);
            updateModuleAudit(conn, 500, "Failure", e.getLocalizedMessage(), insertedKey, startexecutionTime, totalFileRecordsCount, totalFileCount);//numberOfRecord ,long totalFileCount
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("Not able to close the connection");
            }
        }
    }

    private static void checkFilePresence() {
        if (value == -1) {
            File directory = new File(inputLocation + "/" + operatorName + "/" + sourceName);
            value = directory.list().length;
            logger.info("Total File Count:" + value + " " + new File(directory, "null").exists());
            if (value == 0) {
                logger.info("No file present. auditing and exiting ");
                String currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                FilePreProcessing.insertReportv2("O", "0", 0L, 0L, 0L, 0L,
                        currentTime, currentTime, (float) 0, (float) 0, operatorName, sourceName,
                        0L, tag, 0, headCount, servername,0L);
                int insertedKey = ModulesAudit.insertModuleAudit(conn, sourceName.equalsIgnoreCase("all") ? "P2" : "P1", operatorName + "_" + sourceName, servername);
                updateModuleAudit(conn, 200, "Success", "", insertedKey, new Date().getTime(), 0, 0);
                System.exit(0);
            }
        }
    }

}

