# Hebrew bigram map-reduce analysis

#### Personal Information
- Names: Itay Shaish and Daniel Moreh
- IDs: 206446536 and 208276774

## Introduction
This project is a map-reduce analysis of bigrams in Hebrew text.\
The project is implemented in java and uses the AWS Hadoop map-reduce system.\
The project is part of BGU "Big Data" course.

## How To Run
 - Build jars using intellij.
 - Create a bucket on AWS called `bucket1638974297771`
 - Create `Jars` folder in the bucket and upload the jars to it.(the jars are located in the `out/artifacts/Steps` folder)
 - Run the App.jar by running `java -jar App.jar <minNPMI> <relNPMI>`

## Flow of the project
marks:
- N - number of bigrams in the decade
- Cw1w2 - number of occurrences of the bigram w1w2
- Cw1 - number of occurrences of bigram that word w1 was the first word
- Cw2 - number of occurrences of bigram that word w2 was the second word
# Step 1: 
- Short explanation: 
    - The first step is to count the occurrences of each bigram in the text.
    - plus count the number of bigrams in each decade.
- mapper: 
  - Input: <key, value> -> <line number, line> = `<long, "w1 w2    year    occurrences    ignore>"`
  - Output 1: <key, value> -> `<decade, occurrences>`
  - Output 2: <key, value> -> `<decade w1 w2, occurrences>`
- reducer:
    - Output 1: <key, value> -> `<decade, N>`
    - Output 2: <key, value> -> `<decade w1 w2, Cw1w2>`

# Step 2:
- Short explanation: 
    - The second step is to count the occurrences of each bigram that w1 stated.
- mapper:
  - Input 1: <key, value> -> `<decade, N>`
  - Input 2: <key, value> -> `<decade w1 w2, Cw1w2>`
  - Output 1: <key, value> -> `<decade    w1, Cw1w2>`
  - Output 2: <key, value> -> `<decade    w1    w2, N    Cw1w2>`
- reducer:
    - Output 1: <key, value> -> `<decade    w1, Cw1>`
    - Output 2: <key, value> -> `<decade    w1    w2, N    Cw1w2>`

# Step 3:
- Short explanation: 
    - The third step is similar to the second step, but for Cw2.
- mapper:
  - Input 1: <key, value> -> `<decade    w1, Cw1>`
  - Input 2: <key, value> -> `<decade    w1    w2, N    Cw1w2>`
  - Output 1: <key, value> -> `<decade    w2, Cw1w2>`
  - Output 2: <key, value> -> `<decade    w2    w1, N    Cw1w2    Cw1>` (notice that w1 and w2 are swapped in the key)
- reducer:
    - Output 1: <key, value> -> `<decade    w2, Cw2>`
    - Output 2: <key, value> -> `<decade    w2    w1, N    Cw1w2    Cw1>`

# Step 4:
- Short explanation: 
    - The fourth step is to calculate the NPMI for each bigram.
- mapper:
  - Input 1: <key, value> -> `<decade    w2, Cw2>`
  - Input 2: <key, value> -> `<decade    w2    w1, N    Cw1w2    Cw1>`
  - Output 1: <key, value> -> `<decade, w1    w2    N    Cw1w2    Cw1    Cw2>`
- reducer:
    - Output 1: <key, value> -> `<decade    w1    w2, NPMI>`
    - Output 2: <key, value> -> `<decade, SumNPMIofDecade>`

# Step 5:
- Short explanation: 
    - The fifth step is to extract the bigrams that are collocations. (The user passes a minimum NPMI value and relative NPMI value)
- mapper:
  - Input 1: <key, value> -> `<decade    w1    w2, NPMI>`
  - Input 2: <key, value> -> `<decade, SumNPMIofDecade>`
  - Output 1: <key, value> -> `<decade, SumNPMIofDecade>`
  - Output 2: <key, value> -> `<decade    NPMI, w1    w2>` (the partitioner is according to the decade, and the sorting is according to both)
- reducer:
    - Output 1: <key, value> -> `<decade   w1    w2, NPMI>`\
      (only the bigrams that are collocations are passed to the final output)

  
