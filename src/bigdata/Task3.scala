package bigdata

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.{SparkConf, SparkContext}

object Task3 {
	def main(args: Array[String]): Unit = {
		
		val conf = new SparkConf().setAppName("Task3")
		val sc = new SparkContext(conf)
		
		var input_path = args(0) 	//"task1_result"
		val output_path = args(1) 	//"task3_result"
		
		
		//val tmp  = sc.parallelize(List((0,1),(0,2),(1,2),(1,3),(1,4),(2,3)))
		val tmp = sc.textFile(input_path).map(x=>(x.split("\t")(0).toInt, x.split("\t")(1).toInt))
		val degree = tmp.flatMap(x=>List(x._1,x._2)).countByValue
		val edge = tmp.map(x=>if(degree(x._1) <= degree(x._2)) (x._1,x._2) else (x._2,x._1))
		val wedge = edge.join(edge).filter(x=>degree(x._2._1) <= degree(x._2._2)).map(x=>(x._2,x._1))
		val result = wedge.join(edge.map(x=>(x,"$"))).flatMap(x=>List((x._1._1,1), (x._1._2,1), (x._2._1,1))).reduceByKey((x,y)=>x+y)
		result.map(x=>s"${x._1}\t${x._2}").saveAsTextFile(output_path)				
		
	}
}	
					
		