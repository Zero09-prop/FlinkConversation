import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSource {

  def generateRandomStringSource(out:SourceContext[String]): Unit = {
    val lines = Array("how are you","you are how", " i am fine")
    while (true) {
      val index = Random.nextInt(3)
      Thread.sleep(2000)
      out.collect(lines(index))
    }
  }


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val customSource = env.addSource(generateRandomStringSource _)

    customSource.print()

    env.execute()
  }
}