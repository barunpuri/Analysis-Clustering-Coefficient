package bigdata

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
	def main(args: Array[String]): Unit = {
		
		val conf = new SparkConf().setAppName("Task4")
		val sc = new SparkContext(conf)
		
		val input_path1 = args(0)	//"task2_result/*"
		val input_path2 = args(1)	//"task3_result/*"
		val output_path = args(2)	//"task4_result/"
		
		
		val count = sc.textFile(input_path1).map(x=>(x.split("\t")(0).toInt, x.split("\t")(1).toInt))
		val tri = sc.textFile(input_path2).map(x=>(x.split("\t")(0).toInt, x.split("\t")(1).toInt))
		
		tri.join(count).map(x=>s"${x._1}\t${x._2._1.toFloat/x._2._2.toFloat}").saveAsTextFile(output_path)	
		
		
	}
		
}
		