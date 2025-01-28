import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;

import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;

public class Step5 {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder
                                                .standard()
                                                .withRegion("us-east-1")
                                                .build();

    private static List<String> listPartFiles(String bucketName, String s3InputFolder)
            throws AmazonClientException {

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

        return partFiles;
    }

    private static File createArffFile(List<String> partFiles, String bucketName) {
        File arffFile = new File("/tmp/step5_data.arff");
        try (BufferedWriter arffWriter = new BufferedWriter(new FileWriter(arffFile))) {

            writeArffHeader(arffWriter);

            // Read the content of each part file from S3 and write to ARFF
            for (String key : partFiles) {
                try (S3Object s3Object = s3Client.getObject(bucketName, key);
                     S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
                     BufferedReader br = new BufferedReader(new InputStreamReader(s3ObjectInputStream))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        processLine(line, arffWriter);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to write ARFF file: " + e.getMessage());
        }

        return arffFile;
    }

    private static void writeArffHeader(BufferedWriter writer) throws IOException {

        String[] attributes = { "freq_distManhattan", "freq_distEuclidean", "freq_simCosine", "freq_simJaccard",
                "freq_simDice", "freq_simJS",
                "prob_distManhattan", "prob_distEuclidean", "prob_simCosine", "prob_simJaccard", "prob_simDice",
                "prob_simJS",
                "PMI_distManhattan", "PMI_distEuclidean", "PMI_simCosine", "PMI_simJaccard", "PMI_simDice", "PMI_simJS",
                "t-test_distManhattan", "t-test_distEuclidean", "t-test_simCosine", "t-test_simJaccard",
                "t-test_simDice", "t-test_simJS" };

        writer.write("@relation semantic_similarity\n\n");
        for (String attribute : attributes) {
            writer.write("@attribute " + attribute + " numeric\n");
        }
        writer.write("@attribute class {similar, not-similar}\n\n");
        writer.write("@data\n");
    }


    private static void processLine(String line, BufferedWriter writer) throws IOException {
        if (line == null || !line.contains("\t")) {
            System.err.println("[ERROR] Invalid line format: Missing tab separator.");
        }

        String[] parts = line.split("\t", 2);
        if (parts.length != 2) {
            System.err.println("Invalid line format: Expected two parts separated by a tab.");
        }

        String[] keyParts = parts[0].split("\\s+");
        if (keyParts.length != 3) {
            System.err.println("Invalid key format: Expected 'word1 word2 isRelated'.");
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

    }

    private static File evaluateClassifier(File arffFile) {
        File outputFile = new File("/tmp/step5_output.txt");

        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile))) {
            // Load data
            Instances data = DataSource.read(arffFile.getAbsolutePath());
            data.setClassIndex(data.numAttributes() - 1);

            int numFolds = Math.min(10, data.numInstances());
            if (numFolds < 2) {
                System.err.println("[WARN] Dataset must have at least 2 instances for cross-validation. Skipping.");
                return outputFile;
            }

            // Index of "similar" in the class attribute
            int trueIndex = data.classAttribute().indexOfValue("similar");
            if (trueIndex < 0) {
                System.err.println("[WARN] The 'similar' class value was not found in the ARFF class definition.");
            }

            // Classifier and evaluation
            Classifier cls = new RandomForest();
            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(cls, data, numFolds, new Random(42));

            // Write results
            outputWriter.write("=== RandomForest ===\n");
            outputWriter.write("Using " + numFolds + "-fold cross-validation\n\n");

            outputWriter.write(eval.toSummaryString() + "\n");

            if (trueIndex >= 0) {
                double precision = eval.precision(trueIndex);
                double recall = eval.recall(trueIndex);
                double f1 = eval.fMeasure(trueIndex);

                outputWriter.write(String.format("%nPrecision (similar): %.4f%n", precision));
                outputWriter.write(String.format("Recall    (similar): %.4f%n", recall));
                outputWriter.write(String.format("F1        (similar): %.4f%n%n", f1));
            }

            outputWriter.write(eval.toClassDetailsString() + "\n");
            outputWriter.write(eval.toMatrixString() + "\n");

        } catch (Exception e) {
            System.err.println("[ERROR] Failed to evaluate classifier: " + e.getMessage());
        }

        return outputFile;
    }

    private static void uploadResults(String bucketName, File arffFile, File outputFile, String s3OutputFolder) throws AmazonClientException {
        // Upload ARFF
        String resultKey = s3OutputFolder + "step5_result.arff";
        s3Client.putObject(bucketName, resultKey, arffFile);
        System.out.println("[DEBUG] ARFF file uploaded to s3://" + bucketName + "/" + resultKey);

        // Upload output
        String outputFileKey = s3OutputFolder + "step5_output.txt";
        s3Client.putObject(bucketName, outputFileKey, outputFile);
        System.out.println("[DEBUG] Output file uploaded to s3://" + bucketName + "/" + outputFileKey);
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println("Usage: Step5 <bucketName> <inputFolder> <outputFolder>");
            System.exit(1);
        }

        String bucketName = args[1];
        String s3InputFolder = args[2];
        String s3OutputFolder = args[3];

        try{

            List<String> partFiles = listPartFiles(bucketName, s3InputFolder);

            File arffFile = createArffFile(partFiles, bucketName);

            File analysisFile = evaluateClassifier(arffFile);

            uploadResults(bucketName, arffFile, analysisFile, s3OutputFolder);

        }catch (AmazonClientException e) {
            // S3 or AWS related exceptions
            System.err.println("[ERROR] AWS S3 error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            // Catch-all for anything else (e.g., Weka exceptions)
            System.err.println("[ERROR] Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
