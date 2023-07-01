package com.dmat.lambda;


import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Calendar;

public class S3EventHandler implements RequestHandler<S3Event, Boolean> {
    private static final AmazonS3 s3Client = AmazonS3Client.builder()
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build(); 
    
    @Override
    public Boolean handleRequest(S3Event input, Context context) {
        final LambdaLogger logger = context.getLogger();

       logger.log("S3EventHandler:::::::inside handleRequest()");
       String fileNameStr =     input.getRecords().get(0).getS3().getObject().getKey().toString().trim(); 
       int fileSize =     input.getRecords().size();
       String fileSizeStr = String.valueOf(fileSize);
       Calendar cal = Calendar.getInstance();
       Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
       
       try {
    	   logger.log("Connecting to database");
		      String url = "jdbc:postgresql://database-1.cqbnabi7cpag.us-east-1.rds.amazonaws.com:5432/initial_db";
		      String username = "postgres";
		      String password = "password";
		 
		      Connection conn = DriverManager.getConnection(url, username, password);
		
		     String sql = "insert into DMAT_FILE_LOGS_TBL (FILE_NAME,FILE_SIZE,FILE_RECEIVED) VALUES (?,?,?)";
		      PreparedStatement prepStmt = conn.prepareStatement(sql);
		  
		      prepStmt.setString(1, fileNameStr);
		      prepStmt.setString(2, fileSizeStr);
		      prepStmt.setTimestamp(3, timestamp);
		      prepStmt.executeUpdate(); 
				/*
				 * prepStmt.close(); conn.close();
				 */           
       }
		      catch (Exception e) {
		      e.printStackTrace();
		      logger.log("Caught exception: " + e.getMessage());
		    }
       
      
        for(S3EventNotification.S3EventNotificationRecord record: input.getRecords()){
        	String bucketName = record.getS3().getBucket().getName();
            String objectKey = record.getS3().getObject().getKey();
            
   
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream inputStream = s3Object.getObjectContent();

            try(final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))){
                br.lines().skip(1)
                        .forEach(line -> logger.log(line + "\n"));
            } catch (IOException e){
                logger.log("Error occurred in Lambda:" + e.getMessage());
                return false;
            }
            logger.log("Before Initiating copy to processed Folder");
            s3Client.copyObject(bucketName, objectKey, "lakshmis3bucket-processed", objectKey);
            logger.log("Successfully copied the file to Processed Folder");
        }
         return true;
    } 
    
    
    
    
    
    
    
}