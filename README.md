#Chunking Fastq Files
Fast progress in next generation sequencing of DNA has resulted in the availability of large DNA data sets ready for analysis. However, DNA analysis has become the bottleneck in using these data sets, as it requires powerful and scalable tools to perform the needed analysis. A typical analysis pipeline consists of a number of steps, not all of which can readily scale on a distributed computing infrastructure. In this assignment, you will create a framework that implements an in-memory distributed version of the GATK DNA analysis pipeline using Apache Spark. 

The first step is to divide the two provided FASTQ input files into many smaller chunks that are used as input to the parallelized GATK Spark pipeline. The two input files are placed at /data/spark/fastq on the Kova machine. If you do not want to use Kova for this assignment, you can also download these two files from https://www.dropbox.com/s/f2i5g1yyckcgod3/fastq.tar.gz?dl=0 to the computer where you want to run this program. You have to write a program in Scala using Spark, that creates interleaved chunks from data of these two files. Note that each DNA short read consists of 4 lines in the FASTQ files. You have to interleave such that the first read of fastq2.fq should come immediately after the first read of fastq1.fq, the second of fastq2.fq after the second of fastq1.fq and so on. Therefore, the interleaved content would look as follows. 
Read1_of_fastq1.fq 
Read1_of_fastq2.fq 
Read2_of_fastq1.fq 
Read2_of_fastq2.fq 
Read3_of_fastq1.fq 
ReadN_of_fastq1.fq 
ReadN_of_fastq2.fq 

You have to use the following template for this part. https://www.dropbox.com/sh/77h78rsyzuda1bs/AAA7uOR41ZMoNnwzlCqOef3Aa?dl=0 The run bash script for this code takes three arguments. The first being the number of chunks (number of parallel tasks used in the program must be equal to the number of chunks) to make (we recommend making 8 chunks), the second one being the input folder where the fastq files are placed, and the third one being the output folder where the output chunks would be placed. The output chunks must be compressed using gzip by your program. Therefore, there extension would be .fq.gz. You are not allowed to modify the FASTQ files by inserting delimiters. This means, you must be able to separate reads without using any delimiters. Moreover, any form of sequential processing is not allowed. Furthermore, all the chunks must be written in parallel using either of foreachPartitions, mapPartitions and mapPartitionsWithIndex functions.
