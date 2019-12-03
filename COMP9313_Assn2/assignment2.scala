
val inputFilePath  = "/Users/CJMac/Desktop/COMP9313/COMP9313_Assn2/sample_input.txt"
val outputDirPath = "/Users/CJMac/Desktop/COMP9313/COMP9313_Assn2/output"

def convert (a : String) : Int = {

    if (a.endsWith("KB")) {
    	return a.replaceAll("KB","").toInt * 1024
    } else if(a.endsWith("MB")) {
    	return a.replaceAll("MB","").toInt * 1024 * 1024
    } else {
    	return a.replaceAll("B","").toInt
	}
}

def vvariance (list: List[Int]) : Long = {

    val list_mean:Long = list.sum/list.length
    var result:Long = 0
    for (element <- list)
      result += (element - list_mean) * (element - list_mean)
    return (result/list.length).toLong
}

val lines = sc.textFile(inputFilePath, 1)
val columns = lines.map(line => (line.split(",")(0),line.split(",")(3)))

val digits_convert = columns.mapValues(x => convert(x)).mapValues(x => List[Int](x))
val reducebykey = digits_convert.reduceByKey((a, b) => (a ::: b)).sortByKey()

val pre_result = reducebykey.mapValues(x => List(x.min, x.max, x.sum/x.length, vvariance(x)))
val tostring = pre_result.mapValues(_.map(_.toString + "B")).mapValues(_.mkString(","))
val final_result = tostring.map{case(k,v) => s"$k,$v"}
final_result.saveAsTextFile(outputDirPath)


