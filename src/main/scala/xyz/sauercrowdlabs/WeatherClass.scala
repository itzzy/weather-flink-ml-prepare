package xyz.sauercrowdlabs

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.json4s._

class WeatherClass extends DeserializationSchema[WeatherEntry] with SerializationSchema[WeatherEntry]{
  override def deserialize(message: Array[Byte]): WeatherEntry = ???
    //parse()
  //}

  override def isEndOfStream(nextElement: WeatherEntry): Boolean = ???

  override def serialize(element: WeatherEntry): Array[Byte] = ???

  override def getProducedType: TypeInformation[WeatherEntry] = ???
}
