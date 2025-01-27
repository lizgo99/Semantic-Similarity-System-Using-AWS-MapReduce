# System Design for Semantic Similarity Using Map-Reduce
by Liz Gokhvat [username: lizgo , id: 208005777] , Ido Toker [username: idoto , id: 207942186]

## Overview
This system is designed to process a large corpus (Google Syntactic N-grams dataset) to compute semantic similarity between word pairs, build a classifier, and evaluate the performance of that classifier using 10-fold cross validation. The workflow utilizes the MapReduce framework for scalability and efficiency, ensuring robust and distributed processing of massive datasets. 


RandomForest was selected as it consistently delivered the best results, demonstrating high precision and recall during evaluation across diverse configurations.

## Step Descriptions

### Step 1: Initial Processing and Count Calculation

Input: Google Syntactic N-grams dataset in the format: head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year.

Output: Stemmed and tokenized n-grams with extracted lexeme-feature pairs.

Map

Input Key: The line number (LongWritable)

Input Value: A single line of text containing [head_word, syntactic-ngram, total_count, counts_by_year]

Mapper Output Key: Text, prefixed by one of l, f, or lf along with the relevant string (word, feature, or combined)

Mapper Output Value: Text containing the count

Reduce

Input Key: Text of format l <lex>, f <feature>, or lf <lex> <feature>

Input Value: The list of string counts

Reducer Output Key: The same Text key

Reducer Output Value: The sum of the counts

### Step 2: Joining Lexemes and Features

### Step 3: Computing Association Measures

### Step 4: Constructing Feature Vectors

### Step 5: Classification


## Running the Project
1. Update the `App.java` file with your AWS credentials and S3 bucket name.
2. Compile and Package the code:
   ```
   mvn clean package
   ```
3. Upload the compiled JARs of the steps to the S3 bucket.
    - ensure that the location of the JARs is correct in the `App.java` file.
    - ensure that there are no output folders under the same name as the output folders that are in the code.
4. Execute the `Location in your files/App.jar` to launch the EMR job:
   ```
   java -jar target/App.jar
   ```

## Reports


| Run                         | with local aggregation        | without local aggregation       |
|-----------------------------|-------------------------------|---------------------------------|
| Key-Value Pairs to Reducers | 4,145,215                     | 205,621,932                     |
| Size of Data to Reducers    | 58,258,235 bytes (~55.55 MB)  | 4,211,126,503 bytes (~4.21 GB)  |