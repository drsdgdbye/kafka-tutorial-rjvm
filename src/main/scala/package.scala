package com
import com.example.domain.Order
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.jawn.decode
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes

package object example {
	// Type A does not conform to lower bound Null of type parameter T, поэтому A >: Null
 implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
	 val serializer = (a: A) => a.asJson.noSpaces.getBytes()
	 val deserializer = (bytes: Array[Byte]) => decode[A](new String(bytes)).toOption // decode возвращает Either
	 
	 Serdes.fromFn[A](serializer, deserializer)
 }
}
