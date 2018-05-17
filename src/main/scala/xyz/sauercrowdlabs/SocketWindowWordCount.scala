package xyz.sauercrowdlabs

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.json4s.native.JsonMethods
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.shaded.org.joda.time.DateTime
import org.apache.flink.util.Collector
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

case class Weather(
                    iterationTime: String,
                    description: String,
                    time: String,
                    lat: Double,
                    lon: Double,
                    id: Int,
                    name: String,
                    temp: Double,
                    tempMin: Double,
                    tempMax: Double,
                    pressure: Double,
                    humidity: Double,
                    visibility: Double,
                    windSpeed: Double,
                    windDeg: Double,
                    clouds: Double)


object Job {
  implicit val formats = org.json4s.DefaultFormats
  //implicit val formats = Serialization.formats(NoTypeHints)
  def main(args: Array[String]) : Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000) // create a checkpoint every 5 seconds
    env.setStateBackend(new FsStateBackend("file:///home/jonas/flink-checkpoints"))

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")

    val source = env.addSource( new FlinkKafkaConsumer010("weather", new SimpleStringSchema(), props))
      .flatMap(raw => JsonMethods.parse(raw).toOption)
    val parsedSource = source.map(_.extract[Weather])//.assignTimestampsAndWatermarks(new WeatherWatermark())

    parsedSource.writeAsCsv("file:///home/jonas/weather.csv")
//    val w: DataStream[Array[Weather]] = parsedSource.keyBy(x => x.iterationTime)
//      .window(SlidingProcessingTimeWindows.of(Time.days(1), Time.minutes(5)))
//      .process(new WeatherWindowProcessFunction())
//
//    val producer = new FlinkKafkaProducer010[String]("localhost:9092","weather_parsed",)
//    w.writeToSocket("localhost",53123, new WeatherSchema())

    env.execute("Weather ML")

  }
}

class WeatherWindowProcessFunction extends ProcessWindowFunction[Weather,Array[Weather], String, TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[Weather], out: Collector[Array[Weather]]): Unit = {
    val weatherRecords = elements.toArray
    out.collect(weatherRecords)
  }
}

class WeatherSchema extends SerializationSchema[Array[Weather]]{

  override def serialize(element: Array[Weather]): Array[Byte] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    return write(element).getBytes()

  }
}

class WeatherWatermark extends AssignerWithPeriodicWatermarks[Weather]{
  val maxTimeLag = 5000L // 5 seconds

  override def extractTimestamp(element: Weather, previousElementTimestamp: Long): Long = {
    val d = DateTime.parse(element.iterationTime)
    return d.getMillis
  }

  override def getCurrentWatermark: Watermark = {
    return new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}
