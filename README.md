# Analysis-Clustering-Coefficient
Analysis Clustering Coefficient. Final Project in BigData Adv.

## Used Data
<b>LiveJournal Dataset</b>  
http://snap.stanford.edu/data/soc-LiveJournal1.txt.gz  

    $ wget http://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
    $ gunzip soc-LiveJournal1.txt.gz

## Execute Env.
- Google Cloud Platform - DataProc 
- HDFS
- Hadoop MapReduce, Spark 

## How to Execute
### Task 1
    $ hadoop jar task1.jar bigdata.Task1 soc-LiveJournal1.txt task1_result

### Task 2
    $ hadoop jar task2.jar bigdata.Task2 task1_result task2_result

### Task 3
    $ spark-submit --num-executors 10 --conf "spark.default.parallelism=32" –class bigdata.Task3 task3.jar task1_result task3_result

### Task 4
    $ spark-submit --num-executors 10 --class bigdata.Task4 task4.jar task2_result task3_result task4_result 