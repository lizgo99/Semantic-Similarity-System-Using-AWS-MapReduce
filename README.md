# Semantic Similarity System Using MapReduce
by Liz Gokhvat [username: lizgo , id: 208005777] , Ido Toker [username: idoto , id: 207942186]
## Overview

This project implements the algorithm described in 'Comparing Measures of Semantic Similarity' by Nikola Ljubešić et al. with some modifications.
Our program processes a large corpus, the English All - Biarcs dataset from Google Syntactic N-grams, to compute semantic similarity between word pairs. It leverages the MapReduce framework for efficient and scalable processing, and the Random Forest classifier is used for classification tasks. The program's performance is evaluated through 10-fold cross-validation.

## Steps Overview

### Step 1: Initial Processing and Count Calculation

- **Objective:** Process the raw Syntactic N-grams data to compute counts for individual lexemes (`l`), features (`f`), and lexeme-feature pairs (`lf`).

- **Input:** Google Syntactic N-grams dataset (`key: lineID value: head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year`)

- **Output:** Consolidated counts in the format: `{key: l lexeme value: count}` or `{key: f feature value: count}` or `{key: lf lexeme feature value: count}`

1. Mapper parses each line from the input data.
2. Emits counts for lexemes, features, and lexeme-feature pairs.
3. Reducer aggregates counts.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation              | Without Local Aggregation             |
| ---------------- | --------------------------- |-------------------------------------|---------------------------------------|
| **10% Dataset**  | Key-Value Pairs to Reducers | 101,786,379                         | 1,324,992,210                         |
|                  | Size of Data to Reducers    | 1,290,031,230 bytes (\~1.29 GB)     | 21,101,269,246 (\~21.1 GB)            |
| **100% Dataset** | Key-Value Pairs to Reducers | 1,007,362,369                       | 13,544,891,232                        |
|                  | Size of Data to Reducers    | 12,811,842,610 bytes (\~12.81 GB)   | 218,364,691,355 bytes (\~218.36 GB)   |

### Step 2: Data Organization

- **Objective:** Combine counts for lexemes, features , and lexeme-feature pairs  to generate enriched data linking lexemes and features.

- **Input:** Aggregated counts from Step 1 : `{key: lineID value: l lexeme   count}` or `{key: lineID value: f feature   count}` or `{key: lineID value: lf lexeme feature   count}`

- **Output:** Consolidated counts with enriched context in the format `{key: lexeme feature value: lf=_ l=_}` or `{key: lexeme feature value: lf=_ f=_}`

1. Mapper reads and parses counts and emits key-value pairs linking lexemes-features pairs to some of the values (`lf` & `l` or `lf` & `f`).
2. Reducer consolidates counts and contexts for further processing.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation         |
| ---------------- | --------------------------- | ---------------------- |-----------------------------------|
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | 47,984,138                        |
|                  | Size of Data to Reducers    | combiner not used      | 1,692,657,859 (\~1.69 GB)         |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 228,386,555                       |
|                  | Size of Data to Reducers    | combiner not used      | 8,283,378,219 bytes (\~8.28 GB)   |

### Step 3: Computing Association Measures

- **Objective:** Calculate statistical association measures for lexeme-feature pairs, such as frequency, probability, PMI, and t-test scores.

- **Input:** Key-value pairs from Step 2 : `{key: lineID value: lexeme feature   lf=_ l=_}` or `{key: lineID value: lexeme feature   lf=_ f=_}`

- **Output:** Association measures in the format `{key: lexeme-feature pair value: assoc_freq=_ assoc_prob=_ assoc_PMI=_ assoc_t_test=_ }`

1. Mapper emits key-value pairs for word pairs with partial association data.
2. Reducer consolidates, computes and aggregates association metrics.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation       |
| ---------------- | --------------------------- | ---------------------- |---------------------------------|
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | 45,615,530                      |
|                  | Size of Data to Reducers    | combiner not used      | 1,498,289,840 (\~1.5 GB)        |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 221,261,966                     |
|                  | Size of Data to Reducers    | combiner not used      | 7,527,917,604 bytes (\~7.01 GB) |

### Step 4: Constructing Distance Vectors For Classification

- **Objective:** Transform association measures into feature vectors annotated with relatedness labels (`similar` or `not-similar`) for the classification.

- **Input:** Association measures from Step 3 : `{key: lineID value: lexeme-feature pair   assoc_freq=_ assoc_prob=_ assoc_PMI=_ assoc_t_test=_}` and a gold standard dataset.

- **Output:** Annotated feature vectors in the format `{key: lexeme-feature pair, relatedness value: vector data}`

1. Mapper  emits the association measures of the lexeme-feature pairs that are in the golden standard.
2. Reducer computes a 4x6 matrix containing all the possible association measures  and vector-similarities combinations possible and emits it as a vector for the classifier.

**Communication:**

| Dataset Run      | Metric                      | With Local Aggregation | Without Local Aggregation         |
| ---------------- | --------------------------- | ---------------------- |-----------------------------------|
| **10% Dataset**  | Key-Value Pairs to Reducers | combiner not used      | 22,807,765                        |
|                  | Size of Data to Reducers    | combiner not used      | 6,327,696,751 (\~6.3 GB)          |
| **100% Dataset** | Key-Value Pairs to Reducers | combiner not used      | 198,890,689                       |
|                  | Size of Data to Reducers    | combiner not used      | 32,495,273,719 bytes (\~30.26 GB) |

### Step 5: Classification

- **Objective:** Evaluate the performance of the Random Forest classifier on the feature vectors.

- **Input:** Distance vectors from Step 4.

- **Output:** Classification metrics, including precision, recall, and F1 scores.

1. Train and evaluate the Random Forest classifier using 10-fold cross-validation.
2. Evaluate the classifier on the feature vectors.

- This step can be run as the last step of the job flow but it can also be run separately given the data of the output folder of the previous step (step4) as input. To run it separately, use the WekaModel class.

**Results:**

**10% Dataset Summary**

The program ran on `10` files out of the whole dataset. It ran for `~40` minute with 8 instances of `m4.xlarge`.

```
Correctly Classified Instances         13406               93.5063 %
Incorrectly Classified Instances       931                 6.4937 %
Kappa statistic                        0.4467
Mean absolute error                    0.1135
Root mean squared error                0.2263
Relative absolute error                68.0205 %
Root relative squared error            78.3617 %
Total Number of Instances              14337
```

|               | TP Rate | FP Rate | Precision | Recall | F-Measure | MCC   | ROC Area | PRC Area | Class        |
|---------------|---------|---------|-----------|--------|-----------|-------|----------|----------|--------------|
|               | 0.317   | 0.002   | 0.931     | 0.317  | 0.473     | 0.522 | 0.899    | 0.671    | similar      |
|               | 0.998   | 0.683   | 0.935     | 0.998  | 0.965     | 0.522 | 0.899    | 0.985    | not-similar  |
| Weighted Avg. | 0.935   | 0.621   | 0.935     | 0.935  | 0.920     | 0.522 | 0.899    | 0.956    |              |

**Confusion Matrix:**

|                     | Classified as Similar | Classified as Not-Similar |
|---------------------|-----------------------|---------------------------|
| Actual Similar      | 417                   | 900                       |
| Actual Not-Similar  | 31                    | 12989                     |


**100% Dataset Summary**

The program ran on `99` files (the whole dataset). It ran for `~6` hours with 8 instances of `m4.xlarge`.

```
Correctly Classified Instances           13394               93.4226 %
Incorrectly Classified Instances         943                 6.5774 %
Kappa statistic                          0.4308
Mean absolute error                      0.1162
Root mean squared error                  0.2299
Relative absolute error                  69.6369 %
Root relative squared error              79.6004 %
Total Number of Instances                14337
```

|               | TP Rate | FP Rate | Precision | Recall | F-Measure | MCC   | ROC Area | PRC Area | Class        |
|---------------|---------|---------|-----------|--------|-----------|-------|----------|----------|--------------|
|               | 0.300   | 0.002   | 0.950     | 0.300  | 0.456     | 0.513 | 0.893    | 0.652    | similar      |
|               | 0.998   | 0.700   | 0.934     | 0.998  | 0.965     | 0.513 | 0.893    | 0.984    | not-similar  |
| Weighted Avg. | 0.934   | 0.636   | 0.935     | 0.934  | 0.918     | 0.513 | 0.893    | 0.954    |              |

**Confusion Matrix:**

|                     | Classified as Similar | Classified as Not-Similar |
|---------------------|-----------------------|---------------------------|
| Actual Similar      | 395                   | 922                       |
| Actual Not-Similar  | 21                    | 12999                     |

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
