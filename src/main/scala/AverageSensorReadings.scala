import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
case class SensorReading(id: String, timestamp: Long, temperature: Double)
class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](
      Time.seconds(5)
    ) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}
class TemperatureAverager
    extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
      sensorId: String,
      window: TimeWindow,
      vals: Iterable[SensorReading],
      out: Collector[SensorReading]
  ): Unit = {

    // compute the average temperature
    val (cnt, sum) =
      vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
object AverageSensorReadings {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp = sensorData
      .map(r => {
        val celsius = (r.temperature - 32) * (5.0 / 9.0)
        SensorReading(r.id, r.timestamp, celsius)
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(new TemperatureAverager)

    avgTemp.print()

    env.execute("Compute average sensor temmperature")
  }
}
