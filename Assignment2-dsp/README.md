# Assignment2
 knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR).
 
## Introduction
We will generate a knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR). The produced knowledge-base indicates for a each pair of words the probability of their possible next words. In addition, We will  examine the quality of the algorithm according to statistic measures and manual analysis.

## Goal
Our goal is to  build a map-reduce system for calculating the conditional probability of each trigram (w1,w2,w3) found in the corpus:  Hebrew 3-Gram dataset of [Google Books Ngrams](https://aws.amazon.com/datasets/google-books-ngrams/).

The output of the system is a list of word trigrams (w1,w2,w3) and their conditional probabilities (P(w3|w1,w2))).
The list will be ordered: (1) by w1w2, ascending; (2) by the probability for w3, descending.

For example:
```
קפה נמס עלית 0.6
קפה נמס מגורען 0.4

קפה שחור חזק 0.6

קפה שחור טעים 0.3

קפה שחור חם 0.1

…

שולחן עבודה ירוק 0.7

שולחן עבודה מעץ 0.3

…
```
## Team

Mor Bitan 305537383 (morbita)

## Instruction
Have Step1.jar - Step6.jar and Main.jar existing on the s3 bucket : assignment2dspmor .
If all the jars exists , run the Main.jar

## Description
Our application has 6 steps and a Main:

### Step1
Step1 takes as input the 1-gram corpus and parses it line by line.
For each line from the 1-gram corpus , it creates a line with the word and its occurrence , and another line with  * and the same occurrence , and sends it to the reducer.
In the reducer , we combine all the occurences by key. Note that we also sum up all the occurrences with * in order to get the sum of all the words occurences.

### Step2
Step2 takes as input the 2-gram corpus and parses it line by line.
For each line from the 2-gram corpus , it creates a line with the 2 words and their occurrence and sends it to the reducer.
In the reducer , we combine all the occurences by key.

### Step3
Step3 takes as input the 3-gram corpus and parses it line by line.
For each line from the 3-gram corpus , it creates a line with the 3 words and their occurrence and sends it to the reducer.
In the reducer , we combine all the occurences by key.

### Step4
Step4 takes as input Step2 and Step3 output.
If it's the output of Step2, it's a pair and the key will be the pair and the value is the occurence.
If it's the output of Step3 , it's 3 words , so we will split it into 2 pairs.
The first pair is the first and second words. the key is the pair and the value is the 3 words and their occurence.
The second pair is the second and third words. the key is the pair and the value is the 3 words and their occurence.
All this information is sent to the reducer.
In the reducer , the 3 words will become the key , and the value will be the pair and it's occurence.

### Step5
Step5 takes as input Step3 and Step4 output.
If it's the output of Step3, the key is the 3 words and the value is the occurence.
If it's the output of Step4 , the key is the 3 words and the value is the pair and it's occurence.
All the information is sent to the reducer.
In the reducer , we start with the setup function. The setup function loads from the hdfs all Step1 output.
After this , we calculate the probability of the apperance of 3 words in the text.
The way to calculate it is:

<p>
  <img src="https://github.com/eladshamailov/Assignment2/blob/master/probability%20Calculation.png?raw=true"/>
</p>

Where:
* N1 is the number of times w3 occurs.
* N2 is the number of times sequence (w2,w3) occurs.
* N3 is the number of times sequence (w1,w2,w3) occurs.
* C0 is the total number of word instances in the corpus.
* C1 is the number of times w2 occurs.
* C2 is the number of times sequence (w1,w2) occurs.

All the variables are taken from the values in the context.
* N1 - from the map that we initialized in the setup
* N2 - if the value is pair , we take the occurence
* N3 - if the value is the occurence it takes it
* C0 - calculate in the setup , if it * take the occurence
* C1 - from the map that we initialized in the setup
* C2 - take the occurence from the value if its the correct w1 

then , the key is the 3 words and the value is the probability

### Step6
Step6 takes as input the output of Step5.
We compare with the CompareClass two strings , if the first two words are the same , we return wich one has the higher probability.
In the mapper , the output is the same key and value sorted by decending value of the probability.

### Main
The main includes all the steps and the arguments that we need.
We create a cluster , give it all the steps and arguments and we let the application to run.

## Statistics 

### Step1

#### Without local aggregation
```
Map input records=44400490
Map output records=88800536
Map output bytes=860143841
Combine input records=0
Combine output records=0
Reduce input records=88800536
Reduce output records=645262
```

#### With local aggregation
```
Map input records=44400490
Map output records=88800536
Map output bytes=860143841
Combine input records=88800536
Combine output records=645290
Reduce input records=645290
Reduce output records=645262
```

### Step2

#### Without local aggregation
```
Map input records=252069581
Map output records=233334882
Map output bytes=4707102779
Combine input records=0
Combine output records=0
Reduce input records=233334882
Reduce output records=4758874
```  
#### With local aggregation
```
Map input records=252069581
Map output records=233334882
Map output bytes=4707102779
Combine input records=233334882
Combine output records=4758948
Reduce input records=4758948
Reduce output records=4758874
```

### Step3

#### Without local aggregation
```
Map input records=163471963
Map output records=119255104
Map output bytes=2903980410
Combine input records=0
Combine output records=0
Reduce input records=119255104
Reduce output records=2803960
```
#### With local aggregation
```
Map input records=163471963
Map output records=119255104
Map output bytes=2903980410
Combine input records=119255104
Combine output records=2804000
Reduce input records=2804000
Reduce output records=2803960
```

### Step4

#### Without local aggregation
```
Map input records=7562834
Map output records=10366794
Map output bytes=425801365
Combine input records=0
Combine output records=0
Reduce input records=10366794
Reduce output records=5163654
```

### Step5

#### Without local aggregation
```
Map input records=7967614
Map output records=7967614
Map output bytes=296368509
Combine input records=0
Combine output records=0
Reduce input records=7967614
Reduce output records=2972416
```

### Step6

#### Without local aggregation
```
Map input records=2972416
Map output records=2972416
Map output bytes=131663962
Combine input records=0
Combine output records=0
Reduce input records=2972416
Reduce output records=2972416
 ```

## Analysis
example 1:

the pair is: "נתן לה"
```
נתן לה את 0.07483568644656256	
נתן לה . 0.02522314172668169	
נתן לה גט 0.02465862381200599	
נתן לה ביטוי 0.019948799393748117	
נתן לה כסף 0.01733107949119699
```

example 2:

the pair is: "יש שני"
```
יש שני סוגי 0.03916627076408796	
יש שני דברים 0.03148497082510168	
יש שני סוגים 0.028407017568198044	
יש שני צדדים 0.02545576302788053	
יש שני בחינות 0.01399490031724983
```

example 3:

the pair is: "יש קשר"
```
יש קשר בין 0.3135285560511223	
יש קשר הדוק 0.06139292467772331	
יש קשר עם 0.04287247289550313	
יש קשר ישיר 0.03275682770132203	
יש קשר הדוק 0.03193183355020413
```

example 4:

the pair is: "בוודאי אין"
```
בוודאי אין זה 0.07902465668744983	
בוודאי אין כאן 0.028149816679502684	
בוודאי אין לו 0.026563464836665107	
בוודאי אין שום 0.022855927512600346	
בוודאי אין הוא 0.019137767695996497
```

example 5:

the pair is: "את התשובות"
```
את התשובות . 0.08220866570451765	
את התשובות לשאלות 0.06364764039393662	
את התשובות על 0.042019033991377255	
את התשובות הנכונות 0.02915666004956438	
את התשובות של 0.02176840608258283
```

example 6:

the pair is: "צוות של"
```
צוות של שלושה 0.03196954352153084	
צוות של אנשי 0.027098895924006033	
צוות של מומחים 0.022474173406407106	
צוות של עובדים 0.018521259113452416	
צוות של ארבעה 0.01780441474663899
```

example 7:

the pair is: "ראה עצמו"
```
ראה עצמו אחראי 0.01702241799377139	
ראה עצמו חייב 0.016111507206169207	
ראה עצמו כמי 0.015301969969550725	
ראה עצמו מחויב 0.013629409742232276	
ראה עצמו קשור 0.009286008257763649
```

example 8:

the pair is: "שעוד לא"
```
שעוד לא היה 0.052106135938407676	
שעוד לא הגיע 0.026357608847834866	
שעוד לא היתה 0.02302946061966174	
שעוד לא היו 0.01644140334596565	
שעוד לא הגיעה 0.01472695168870489
```

example 9:

the pair is: "שעושה את"
```
שעושה את זה 0.03980628955112537	
שעושה את כל 0.033650071244454426	
שעושה את האדם 0.03346686715935552	
שעושה את התורה 0.013671238326566627	
שעושה את החיים 0.012113286774776006
```

example 10:

the pair is: "תורה שלמה"
```
תורה שלמה . 0.06109499697707543	
תורה שלמה שלנו 0.05686185190579819	
תורה שלמה של 0.033955895253863645	
תורה שלמה היא 0.03043921157562914	
תורה שלמה על 0.03008306261598419
```

## Output
the Output file can be downloaded here: https://s3-us-west-2.amazonaws.com/assignment2dspmor/OurOutput
