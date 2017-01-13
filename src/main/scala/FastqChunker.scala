import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner
import org.apache.spark._
import sys.process._
import scala.sys.process.Process
import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
import java.io._
import java.nio.file.{Paths, Files}

object FastqChunker 
{

// helper function to read files in the given inputFolder as 2nd perameter of run script 
def getListOfFiles(dir: String):List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}

def main(args: Array[String]) 
{
	if (args.size < 3)
	{
		println("Not enough arguments!\nArg1 = number of parallel tasks = number of chunks\nArg2 = input folder\nArg3 = output folder")
		System.exit(1)
	}
	
	val prllTasks = args(0)
	val inputFolder = args(1)
	val outputFolder = args(2)
	
	if (!Files.exists(Paths.get(inputFolder)))
	{
	 	println("Input folder " + inputFolder + " doesn't exist!")
	 	System.exit(1)
   }
   println("Input folder " + inputFolder + " doesn't exist!")
		 
	// Create output folder if it doesn't already exist
	new File(outputFolder).mkdirs
	
	println("Number of parallel tasks = number of chunks = " + prllTasks + "\nInput folder = " + inputFolder + "\nOutput folder = " + outputFolder)
	
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	conf.setMaster("local[" + prllTasks + "]")
	conf.set("spark.cores.max", prllTasks)
	
	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	// getting list of files in the InputFolder,  getListOfFiles Function is defined above 
    val fasqFiles = getListOfFiles(inputFolder)
 	
 	// Exiting if there are not 2 files to merg 
 	if (fasqFiles.size < 2)
	{
	 	println("Input folder " + inputFolder + " doesn't have 2 Fastq Files!")
	 	System.exit(1)
    }
    
    // sorting files to keep each read of Fastq File 1 before the Fastq File 2 
    val sorteFilesList = fasqFiles.sorted

    // getting absolute pathr from java.io.File object 
 	val fastQ1_path = sorteFilesList(0).getAbsolutePath()
 	val fastQ2_path = sorteFilesList(1).getAbsolutePath()

    // generating rdd of the each line of file, assigning unique Index to each line, and then making this idex as key and line as value i.e.  
    //                                                             (LongIndex, TXT)   // index starts from zero (0)
 	val fileRdd_1 = sc.textFile(fastQ1_path).zipWithIndex.map(x=> (x._2, x._1))   

 	// deviding each index by 4 (as we need to combine 4 lines as single Gnome record)  so It will assigne 1 unique key to each 4 consecative lines i
 	//  i.e. (0, TXT) (0, TXT)  (0, TXT)  (0, TXT)  (1, TXT) (1, TXT) (1, TXT) (1, TXT) ... , reduced them by keys and concatinating the txt of file 2 after each read of file 1, Finally sorting it   	
 	val chrRdd_1 = fileRdd_1.map(x=> (x._1/4, x._2)).reduceByKey( (x, y) => x +"\n"+ y ).sortBy(_._1)

    // same operations for Fastq Files 2 as in above 2 lines  
	val fileRdd_2 = sc.textFile(fastQ2_path).zipWithIndex.map(x=> (x._2, x._1))   
	val chrRdd_2 = fileRdd_2.map(x=> (x._1/4, x._2)).reduceByKey( (x, y) => x +"\n"+ y ).sortBy(_._1)

    // combining rdds of boht files with union, reduced them by keys, i.e  Gnomes record having X key of File 1 will be concatinated after the the  Gnomes record of File 1
	val joinrdd = chrRdd_1.union(chrRdd_2).reduceByKey( (x, y) => x +"\n"+ y ).sortBy(_._1)

    // calculating a balanced partition size to diving output File into given number of prllTasks chunks i.e. 8   
	val  parSize =  (joinrdd.count.toInt/ prllTasks.toInt )-1
    
    // diving each key by partition size (parSize) to indicate to HashPartitioner that wich record should be combined in each partition 
    // first parSize records will in partion 1, next parSize records will be in partition 2 and so on . . .     
    val hashKeyRDD = joinrdd.map(x=> (x._1.toInt/parSize, x._2))
    
    // partioning the rdd using hashPartioner and making each record as string and apending \n that will help to keep the formate of file as it is while writting 
    val rangePartitionRDD = hashKeyRDD.partitionBy(new HashPartitioner(prllTasks.toInt)).map(x=> x._2.toString+"\n")
     
    // writting each partition into ouput chunk into given outputFolder  
    rangePartitionRDD.foreachPartition(iter => {
         			val partitionNb = TaskContext.getPartitionId().toString // getting partition number     		
        			println("Writting File for Partition :: "+partitionNb )
        			val file = scala.tools.nsc.io.Path(s"$outputFolder"+"/chunk" + partitionNb + ".fq").createFile() // creating file for each chunk i.e partition with partition ID 
        			while (iter.hasNext) {
					    file.appendAll(iter.next) // writting to file 
					}
					val zipCmd = s"gzip $file" // preparing command to zip file 
					zipCmd.!                   // command execution using scala process to execute system process 
 				}) 	


    println("Number of Records in each Partition :: ")
    // priting size of each partition on STD output to make sure that partitions are quite balanced !
    rangePartitionRDD.mapPartitions(iter => Iterator(println(iter.length))).collect
	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))

}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
