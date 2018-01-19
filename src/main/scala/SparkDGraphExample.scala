import java.util
import java.util.function.Consumer

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import io.dgraph.{DgraphClient, DgraphGrpc, DgraphProto}
import io.dgraph.DgraphGrpc.DgraphBlockingStub
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.{Collections, Map}

import DGraphExample._
import com.google.gson.Gson
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkDGraphExample {
  val FIND_BY_AGE: String = "query all($age: int){all(func: ge(age, $age)) {\n" + " firstName\n" + "\tlastName\n" + "\tage\n" + "}\n" + "}"
  val FIND: String = "{\n  all(func: ge(age, 12)) {\n  \tfirstName\n  \tlastName\n  \tage\n  }\n}"
  val graph: Graph[String, String] = null
  var dgraphClient: DgraphClient = null

  def getOlderThan(age: Int): People = {
    val gson: Gson = new Gson
    val vars: util.Map[String, String] = Collections.singletonMap("$age", String.valueOf(age))
    val res: DgraphProto.Response = dgraphClient.newTransaction.queryWithVars(FIND_BY_AGE, vars)
    gson.fromJson(res.getJson.toStringUtf8, classOf[People])
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster("local")
    val sc = new SparkContext(conf)

    val channel = ManagedChannelBuilder.forAddress("10.66.170.158", 9080).usePlaintext(true).build
    val blockingStub = DgraphGrpc.newBlockingStub(channel)
    dgraphClient = new DgraphClient(Collections.singletonList(blockingStub))

    val vert = getOlderThan(17).all

    println("$getting: " + vert.size())

    val vertP: Array[Person]  = new Array[Person](vert.size())
    vert.toArray(vertP)
    val builder = Seq.newBuilder[(VertexId, Person)]
    var i: VertexId = 0L

    vertP.foreach(v => builder.+=(({i+=1; i}, v)))
    var vertexRDD = sc.parallelize(builder.result())
    val graph: Graph[Person, String] = Graph(vertexRDD, sc.emptyRDD[Edge[String]] )

    println("$vertices " + graph.vertices.filter(_._2.getAge >= 22).count())

  }
}
