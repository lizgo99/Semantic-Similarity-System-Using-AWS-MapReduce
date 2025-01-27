# README: Semantic Similarity System Using MapReduce
by Liz Gokhvat [username: lizgo , id: 208005777] , Ido Toker [username: idoto , id: 207942186]
## Overview

This system processes a large corpus, the Google Syntactic N-grams dataset, to compute semantic similarity between word pairs. It leverages the MapReduce framework for efficient and scalable processing, and the Random Forest classifier is used for classification tasks. The system's performance is evaluated through 10-fold cross-validation.

## Steps Overview

### Step 1: Initial Processing and Count Calculation

- **Objective:** Process the raw Syntactic N-grams data to compute counts for individual lexemes ('l'), features ('f'), and lexeme-feature pairs ('lf').

- **Input:** Google Syntactic N-grams dataset (`head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year`)

- **Output:** Consolidated counts in the format: `{key: l lexeme value: count}` or `{key: f feature value: count}` or `{key: lf lexeme feature value: count}`

1. Mapper parses each line from the input data.
2. Emits counts for lexemes, features, and lexeme-feature pairs.
3. Reducer aggregates counts.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation            | Without Local Aggregation           |
| ---------------- | --------------------------- | --------------------------------- | ----------------------------------- |
| **10% Dataset**  | Key-Value Pairs to Reducers | ###                               | ###                                 |
|                  | Size of Data to Reducers    | ###                               | ###                                 |
| **100% Dataset** | Key-Value Pairs to Reducers | 1,007,362,369                     | 13,544,891,232                      |
|                  | Size of Data to Reducers    | 12,811,842,610 bytes (\~12.81 GB) | 218,364,691,355 bytes (\~218.36 GB) |

### Step 2: Data Organization

- **Objective:** Combine counts for lexemes, features , and lexeme-feature pairs  to generate enriched data linking lexemes and features.

- **Input:** Aggregated counts from Step 1 : '{key: l lexeme value: count}' or '{key: f feature value: count}' or '{key: lf lexeme feature value: count}'

- **Output:** Consolidated counts with enriched context in the format `{key: lexeme feature value: lf=_ l=_}` or '{key: lexeme feature value: lf=\_ f=\_}'

1. Mapper reads and parses counts and emits key-value pairs linking lexemes-features pairs to some of the values (lf & l or lf & f).
2. Reducer consolidates counts and contexts for further processing.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation       |
| ---------------- | --------------------------- | ---------------------- | ------------------------------- |
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | ###                             |
|                  | Size of Data to Reducers    | combiner not used      | ###                             |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 228,386,555                     |
|                  | Size of Data to Reducers    | combiner not used      | 8,283,378,219 bytes (\~8.28 GB) |

### Step 3: Computing Association Measures

- **Objective:** Calculate statistical association measures for lexeme-feature pairs, such as frequency, probability, PMI, and t-test scores.

- **Input:** Key-value pairs from Step 2 : '{key: lexeme feature value: lf=\_ l=\_}' or '{key: lexeme feature value: lf=\_ f=\_}'

- **Output:** Association measures in the format `{key: lexeme-feature pair value: assoc`*`freq=_ `*`assoc`*`prob=_ `*`assoc`*`PMI=_ `*`assoc_t_test=_}`

1. Mapper emits key-value pairs for word pairs with partial association data.
2. Reducer consolidates, computes and aggregates association metrics.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation       |
| ---------------- | --------------------------- | ---------------------- | ------------------------------- |
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | ###                             |
|                  | Size of Data to Reducers    | combiner not used      | ###                             |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 221,261,966                     |
|                  | Size of Data to Reducers    | combiner not used      | 7,527,917,604 bytes (\~7.01 GB) |

### Step 4: Constructing Distance Vectors For Classification

- **Objective:** Transform association measures into feature vectors annotated with relatedness labels (`similar` or `not-similar`) for the classification.

- **Input:** Association measures from Step 3 : '{key: lexeme-feature pair value: assocfreq=\_ assocprob=\_ assocPMI=\_ assoc\_t\_test=\_}' and a gold standard dataset.

- **Output:** Annotated feature vectors in the format `{key: lexeme-feature pair, relatedness value: vector data}`

1. Mapper  emits the association measures of the lexeme-feature pairs that are in the golden standard.
2. Reducer computes a 4x6 matrix containing all the possible association measures  and vector-similarities combinations possible and emits it as a vector for the classifier.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation         |
| ---------------- | --------------------------- | ---------------------- | --------------------------------- |
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | ###                               |
|                  | Size of Data to Reducers    | combiner not used      | ###                               |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 198,890,689                       |
|                  | Size of Data to Reducers    | combiner not used      | 32,495,273,719 bytes (\~30.26 GB) |

### Step 5: Classification

- **Objective:** Evaluate the performance of the Random Forest classifier on the feature vectors.

- **Input:** Distance vectors from Step 4.

- **Output:** Classification metrics, including precision, recall, and F1 scores.

1. Train the Random Forest classifier using 10-fold cross-validation.
2. Evaluate the classifier on the feature vectors.

**Results:**

| Dataset Run      | Precision | Recall | F1 Measure |
| ---------------- | --------- | ------ | ---------- |
| **10% Dataset**  | ###       | ###    | ###        |
| **100% Dataset** | 0.9495    | 0.2999 | 0.4559     |

The classifier demonstrates high precision but lower recall, indicating it is highly accurate in its predictions but less effective at identifying all positive instances.

## Running the Project

1. Update the `App.java` file with AWS credentials and S3 bucket information.
2. Compile and package the project:
   ```bash
   mvn clean package
   ```
3. Upload JAR files for each step to the S3 bucket.
 - ensure that the location of the JARs is correct in the App.java file.
 - ensure that there are no output folders under the same name as the output folders that are in the code.
4. Execute the main application JAR:
   ```bash
   java -jar target/App.jar
   ```

