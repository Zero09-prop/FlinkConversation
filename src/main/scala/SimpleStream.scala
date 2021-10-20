import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{
  StreamExecutionEnvironment,
  createTypeInformation
}
import org.apache.flink.util.Collector

case class Name(id: Int, name: String)
case class Age(id: Int, age: Int)
case class Person(id: Int, name: String, age: Int)

class MyFlatMap extends RichCoFlatMapFunction[Name, Age, Person] {
  var names: MapState[Int, String] = _
  var ages: MapState[Int, Int] = _
  override def open(parameters: Configuration): Unit = {
    names = getRuntimeContext.getMapState(
      new MapStateDescriptor(
        "names",
        classOf[Int],
        classOf[String]
      )
    )
    ages = getRuntimeContext.getMapState(
      new MapStateDescriptor(
        "ages",
        classOf[Int],
        classOf[Int]
      )
    )
  }
  override def flatMap1(data: Name, out: Collector[Person]): Unit = {
    if (ages.contains(data.id)) {
      out.collect(Person(data.id, data.name, ages.get(data.id)))
      ages.remove(data.id)
    } else {
      names.put(data.id, data.name)
    }
  }
  override def flatMap2(data: Age, out: Collector[Person]): Unit = {
    if (names.contains(data.id)) {
      out.collect(Person(data.id, names.get(data.id), data.age))
      names.remove(data.id)
    } else {
      ages.put(data.id, data.age)
    }
  }

}
object SimpleStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source =
      env.fromCollection(List(Option(1), Option(2), Option(3), None, Option(5)))
    val result = source.flatMap(str => str.toList)
    //result.print()
    val col1 = List(Name(1, "Misha"), Name(2, "Paul"))
    val col2 = List(Age(2, 13), Age(1, 18))
    val source1 = env.fromCollection(col1)
    val source2 = env.fromCollection(col2)
    source1
      .connect(source2)
      .keyBy(d1 => d1.id, d2 => d2.id)
      .flatMap(new MyFlatMap)
      .print()
    env.execute("My first program")
  }
}
