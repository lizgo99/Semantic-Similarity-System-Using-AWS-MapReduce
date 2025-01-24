import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class WekaModelStep {
    
    public static AmazonS3 S3;
    public static String jarBucketName = "classifierinfo1";

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println("Usage: WekaModelStep <bucketName> <inputFolder> <outputFolder>");
            System.exit(1);
        }

        String bucketName = args[1];
        String s3InputFolder = args[2];
        String s3OutputFolder = args[3];

        System.out.println("[INFO] Connecting to aws");
        System.out.println("[INFO] Bucket name: " + bucketName);
        System.out.println("[INFO] Input folder: " + s3InputFolder);
        System.out.println("[INFO] Output folder: " + s3OutputFolder);

        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion("us-east-1")
                .build();

        
        List<String> partFiles = new ArrayList<>();
        ObjectListing objectListing = s3Client.listObjects(bucketName, s3InputFolder);
        do {
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                String key = summary.getKey();
                // Only pick files that start with "part-r"
                System.out.println("Key: " + key);
                if (key.startsWith(s3InputFolder + "part-r")) {
                    System.out.println("Adding: " + key);
                    partFiles.add(key);
                }
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } while (objectListing.isTruncated());
        
        File arffFile = new File("/tmp/step5_data.arff");
        System.out.println("Writing ARFF to: " + arffFile);
        try (BufferedWriter arffWriter = new BufferedWriter(new FileWriter(arffFile))) {

            String[] features = { "freq_distManhattan", "freq_distEuclidean", "freq_simCosine", "freq_simJaccard",
                "freq_simDice", "freq_simJS",
                "prob_distManhattan", "prob_distEuclidean", "prob_simCosine", "prob_simJaccard", "prob_simDice",
                "prob_simJS",
                "PMI_distManhattan", "PMI_distEuclidean", "PMI_simCosine", "PMI_simJaccard", "PMI_simDice", "PMI_simJS",
                "t-test_distManhattan", "t-test_distEuclidean", "t-test_simCosine", "t-test_simJaccard",
                "t-test_simDice", "t-test_simJS" };

            
            System.out.println("Writing ARFF: " + arffFile.getAbsolutePath());
            arffWriter.write("@relation semantic_similarity\n");
            System.out.println("@relation semantic_similarity\n");

            for (String feature : features) {
                arffWriter.write("@attribute " + feature + " numeric\n");
                System.out.println("@attribute " + feature + " numeric\n");
            }
            arffWriter.write("@attribute class {True, False}\n");
            System.out.println("@attribute class {True, False}\n");
            arffWriter.write("@data\n");
            System.out.println("@data\n");

            for (String key : partFiles){
                S3Object s3Object = s3Client.getObject(bucketName, key);
                try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
                    BufferedReader br = new BufferedReader(new InputStreamReader(s3ObjectInputStream))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        processLine(line, arffWriter);
                    }
                }
            }

        }catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        try{
            System.out.println("Reading ARFF: " + arffFile.getAbsolutePath());
            Instances data = DataSource.read(arffFile.getAbsolutePath());
            data.setClassIndex(data.numAttributes() - 1);

            File resultArff = new File("/tmp/step5_result.arff");
            System.out.println("Writing ARFF: " + resultArff.getAbsolutePath());
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(resultArff))) {
                // Suppose we just copy the same data or a “predictions” set. 
                // For simplicity, let's store the same data:
                ArffSaver saver = new ArffSaver();
                saver.setInstances(data);
                saver.setFile(resultArff);
                saver.writeBatch();
            }
            System.out.println("Uploading result ARFF to s3");
            // Upload the resultArff to S3 under the outputFolder
            String resultKey = s3OutputFolder + "step5_result.arff";
            s3Client.putObject(bucketName, resultKey, resultArff);
    
            System.out.println("WekaModelStep completed successfully. ARFF file uploaded to s3://" 
                                + bucketName + "/" + resultKey);
            
        }catch(Exception e){
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

    }

    // line : us about True [302.0, 155.98717896032352, 0.0, 0.0, 0.0,
    // 100.50634118119207, 2.0, 0.9575719294131382, 0.0, 0.0, 0.0,
    // 0.594720280920433, 945.742, 325.0149798824663, 0.0, 0.0, 0.0,
    // 65.72213621915233, 3.5570000000000004, 1.3169984813962392, 0.0, 0.0, 0.0,
    // 0.6356159645734698]
    private static void processLine(String line, BufferedWriter writer) throws IOException {
        if (line == null || !line.contains("\t")) {
            throw new IllegalArgumentException("Invalid line format: Missing tab separator.");
        }

        String[] parts = line.split("\t", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid line format: Expected two parts separated by a tab.");
        }

        String[] keyParts = parts[0].split("\\s+");
        if (keyParts.length != 3) {
            throw new IllegalArgumentException("Invalid key format: Expected 'word1 word2 isRelated'.");
        }
        // String w1 = keyParts[0];
        // String w2 = keyParts[1];
        String isRelated = keyParts[2];

        String values = parts[1]
                .replaceAll("[\\[\\]]", "")
                .replaceAll("\\s+", "");

        String arffLine = String.format("%s,%s%n", values, isRelated);

        writer.write(arffLine);
        System.out.println(arffLine);
    }
}
