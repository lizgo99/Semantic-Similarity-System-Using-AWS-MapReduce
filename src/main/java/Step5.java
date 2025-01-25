import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.*;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;

import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SMO;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class Step5 {
    
    public static AmazonS3 S3;

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println("Usage: Step5 <bucketName> <inputFolder> <outputFolder>");
            System.exit(1);
        }

        String bucketName = args[1];
        String s3InputFolder = args[2];
        String s3OutputFolder = args[3];

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
                if (key.startsWith(s3InputFolder + "part-r")) {
                    partFiles.add(key);
                }
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } while (objectListing.isTruncated());
        
        File arffFile = new File("/tmp/step5_data.arff");
        System.out.println("[DEBUG] Writing ARFF to: " + arffFile);
        try (BufferedWriter arffWriter = new BufferedWriter(new FileWriter(arffFile))) {

            String[] features = { "freq_distManhattan", "freq_distEuclidean", "freq_simCosine", "freq_simJaccard",
                "freq_simDice", "freq_simJS",
                "prob_distManhattan", "prob_distEuclidean", "prob_simCosine", "prob_simJaccard", "prob_simDice",
                "prob_simJS",
                "PMI_distManhattan", "PMI_distEuclidean", "PMI_simCosine", "PMI_simJaccard", "PMI_simDice", "PMI_simJS",
                "t-test_distManhattan", "t-test_distEuclidean", "t-test_simCosine", "t-test_simJaccard",
                "t-test_simDice", "t-test_simJS" };

            System.out.println("[DEBUG] Starting writing ARFF");
            arffWriter.write("@relation semantic_similarity\n");

            for (String feature : features) {
                arffWriter.write("@attribute " + feature + " numeric\n");
            }
            arffWriter.write("@attribute class {similar, not-similar}\n");
            arffWriter.write("@data\n");

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
            System.out.println("[ERROR] " + e.getMessage());
            e.printStackTrace();
        }
        Instances data;
        try{
            System.out.println("[DEBUG] Reading ARFF: " + arffFile.getAbsolutePath());
            data = DataSource.read(arffFile.getAbsolutePath());
            data.setClassIndex(data.numAttributes() - 1);

            String resultKey = s3OutputFolder + "step5_result.arff";
            s3Client.putObject(bucketName, resultKey, arffFile);

            System.out.println("[DEBUG] ARFF file uploaded to s3://" 
                                + bucketName + "/" + resultKey);
                                

            // Create the train and test sets
            final int numInstances = data.numInstances();
            int trainSize = (int) Math.round(numInstances * 0.8);
            int testSize = numInstances - trainSize;

            Instances trainSet = new Instances(data, 0, trainSize);
            Instances testSet = new Instances(data, trainSize, testSize);
            
            
            Classifier[] classifiers = {
                new RandomForest(),
                new Logistic(),
                new SMO(),
                new NaiveBayes()
            };
            String[] classifierNames = {
                    "RandomForest",
                    "Logistic",
                    "SMO (SVM)",
                    "NaiveBayes"
            };

            System.out.println("\n=== Cross-Validation: Compare Classifiers ===\n");
            
            // Determine appropriate number of folds (minimum of 10 and dataset size)
            int numFolds = Math.min(10, data.numInstances());
            if (numFolds < 2) {
                throw new IllegalStateException("Dataset must have at least 2 instances for cross-validation");
            }
            
            // By default, let's treat "similar" as the positive class
            int trueIndex = data.classAttribute().indexOfValue("similar");
            if (trueIndex < 0) {
                System.err.println("[WARNING] The 'similar' class value wasn't found. Check ARFF class definition!");
            }

            for (int i = 0; i < classifiers.length; i++) {
                Classifier cls = classifiers[i];
                String name = classifierNames[i];
    
                try {
                    Evaluation eval = new Evaluation(data);
                    // Use dynamic number of folds
                    eval.crossValidateModel(cls, data, numFolds, new Random(42));
    
                    System.out.println("=== " + name + " ===");
                    System.out.println("Using " + numFolds + "-fold cross-validation");
    
                    // Basic summary
                    System.out.println(eval.toSummaryString());
    
                    // Precision, Recall, F1 specifically for the “similar” class  
                    if (trueIndex >= 0) {
                        double precision = eval.precision(trueIndex);
                        double recall    = eval.recall(trueIndex);
                        double f1        = eval.fMeasure(trueIndex);
    
                        System.out.println("Precision (similar): " + String.format("%.4f", precision));
                        System.out.println("Recall    (similar): " + String.format("%.4f", recall));
                        System.out.println("F1        (similar): " + String.format("%.4f", f1));
                    }
    
                    // Optional: detailed stats, confusion matrix
                    System.out.println(eval.toClassDetailsString());
                    System.out.println(eval.toMatrixString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("--------------------------------------------------\n");
            }
    
            System.out.println("[INFO] Done! Evaluated all classifiers with cross-validation.\n");
            
        }catch(Exception e){
            System.out.println("[ERROR] " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processLine(String line, BufferedWriter writer) throws IOException {
        if (line == null || !line.contains("\t")) {
            throw new IllegalArgumentException("[ERROR] Invalid line format: Missing tab separator.");
        }

        String[] parts = line.split("\t", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid line format: Expected two parts separated by a tab.");
        }

        String[] keyParts = parts[0].split("\\s+");
        if (keyParts.length != 3) {
            throw new IllegalArgumentException("Invalid key format: Expected 'word1 word2 isRelated'.");
        }

        String word1 = keyParts[0];
        String word2 = keyParts[1];
        boolean isRelated = Boolean.parseBoolean(keyParts[2]);
        String isSimilar = isRelated ? "similar" : "not-similar";

        String values = parts[1]
                .replaceAll("[\\[\\]]", "")
                .replaceAll("\\s+", "");

        String arffLine = String.format("%s,%s%n", values, isSimilar);

        writer.write(arffLine);
        System.out.println(String.format("[DEBUG] %s %s %s %n", word1, word2, isSimilar));
        System.out.println("[DEBUG] " + arffLine);
    }
}
