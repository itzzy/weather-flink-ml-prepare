package xyz.sauercrowdlabs

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.json4s.native.JsonMethods
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

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

  def main(args: Array[String]) : Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000) // create a checkpoint every 5 seconds
    //env.getConfig.setGlobalJobParameters(params) // make parameters available in the web interface

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")

    val source = env.addSource( new FlinkKafkaConsumer010("weather", new SimpleStringSchema(), props))
      .flatMap(raw => JsonMethods.parse(raw).toOption)
    val parsedSource = source.map(_.extract[Weather])

    parsedSource.keyBy(x => x.iterationTime).timeWindow(Time.minutes(5))
    parsedSource.keyBy(1).t

    env.execute("Kafka 0.10 Example")

  }
}
