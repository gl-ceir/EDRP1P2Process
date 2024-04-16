package com.gl.reader.dto;

import com.gl.reader.constants.Alerts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

import static com.gl.reader.service.ProcessController.*;

public class FilePreProcessing {
    static Logger logger = LogManager.getLogger(FilePreProcessing.class);

    public static void insertReportv2(String fileType, String fileName, Long totalRecords, Long totalErrorRecords,
                                      Long totalDuplicateRecords, Long totalOutputRecords, String startTime, String endTime, Float timeTaken,
                                      Float tps, String operatorName, String sourceName, long volume, String tag, Integer FileCount,
                                      Integer headCount, String servername) {

        logger.info("Output File Report Final Out FileName: " + fileName + ", Date: " + LocalDateTime.now() + ", Start Time: "
                + startTime + ", End Time: " + endTime + ", Time Taken: " + timeTaken
                + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps
                + ", Error: " + totalErrorRecords + ", inSet: " + totalOutputRecords + ", totalCount: " + totalErrorRecords
                + ", duplicate: " + totalDuplicateRecords + ", volume: " + volume + ", tag: " + tag + "; File Processed  " + FileCount);
        logger.debug("Connection:::::" + conn);
        try (Statement stmt = conn.createStatement();) {
            endTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
            if (fileType.equalsIgnoreCase("O")) {
                headCount = headCount + 1;
            }
            String dateFunc = defaultStringtoDate(procesStart_timeStamp);
            String sql = "Insert into " + edrappdbName
                    + ".edr_file_pre_processing_detail(CREATED_ON,MODIFIED_ON,FILE_TYPE,TOTAL_RECORDS,TOTAL_ERROR_RECORDS,TOTAL_DUPLICATE_RECORDS,TOTAL_OUTPUT_RECORDS,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername )"
                    + "values(" + dateFunc + " , CURRENT_TIMESTAMP , '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
                    + totalDuplicateRecords + "," + totalOutputRecords + ",'" + fileName + "'," + defaultStringtoDate(startTime) + ","
                    + defaultStringtoDate(endTime) + "," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "'    )";
            logger.info("Inserting into table  pre_processing  _report:: " + sql);
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            Alert.raiseAlert(Alerts.ALERT_006, Map.of("<e>", "not able to insert in file_pre_processing_detail " + e.toString() + ". in   ", "<process_name>", "EDR_pre_processor"), 0);
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
            return "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')"; // commented by sharad

        } else {
            return "now()";
        }
    }


}
