import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by r.benrejeb on 19/11/2014.
 */
object CountHeight extends App{

  val sparkConf = new SparkConf().setAppName("arbres-alignement").setMaster("local")
  val sc= new SparkContext(sparkConf)
  val cache=sc.textFile("arbresalignementparis2010.csv")
    .filter(line => !line.startsWith("geom")&&line.length>0)
    .map(line => line.split(";"))
    .map(fields => fields(3).toFloat)
    .filter(height => height > 0).cache()
  val count:Long=cache.map(x=>1).reduce((x,y)=>x+y)



  val sum:Float=cache.reduce((x,y)=>x+y);
  println("Count :"+count)
  println("Sum :"+sum)


}
