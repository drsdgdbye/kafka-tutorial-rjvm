package com.example

import domain.Domain._
import domain._
import kafka.Topics._

import io.circe.generic.auto._
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, Printed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties


object KafkaStreams extends App {
	val builder = new StreamsBuilder()
	/** KStream is a linear flow from the kafka topic */
	val userOrderStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser)
	val expensiveF = (_: UserId, order: Order) => order.amount >= 1000
	val expensiveOrders = userOrderStream.filter(expensiveF)
	val listOfProducts: KStream[UserId, List[Product]] = userOrderStream.mapValues(_.products)
	val productsStream: KStream[UserId, Product] = userOrderStream.flatMapValues(_.products)
	
	/**
	 * KTable is аналог кафка стрим за исключением того, что Таблицы представляют собой набор изменяющихся фактов.
	 * Каждое новое событие перезаписывает старое, тогда как потоки представляют собой набор неизменных фактов.
	 * В настройках топика нужно указать cleanup.policy=compact. всего может быть 3 типа cleanup.policy - delete, compact, 'compact, delete'
	 */
	val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfiliesByUser)
	
	/**
	 * GlobalKTable - copied to all the nodes.
	 * It used for broadcast information to all tasks or to do joins without re-partitioned the input data.
	 * */
	val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)
	
	// join stream with table. объединяет стрим с таблицей(или таблица с таблицей) по значениям (a, b).join(c, d)((b, d) => (a, (b, d)))
	val ordersWithUserProfiles: KStream[UserId, (Order, Profile)] = userOrderStream.join(userProfilesTable)((_, _))
	
	val discountedOrdersStream: KStream[UserId, Order] =
		ordersWithUserProfiles.join(discountProfilesGTable)(
			{ case (userId, (order, profile)) => profile },
			// ключ берётся из ordersWithUserProfiles
			{ case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) }
			// значения берутся из подходящих записей
		)
	
	// pick another id
	val ordersStream = discountedOrdersStream.selectKey((userId, order) => order.orderId)
	val paymentsStream = builder.stream[OrderId, Payment](Payments)
	val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(5, ChronoUnit.MINUTES))
	val joinOrderPaymentsF: (Order, Payment) => Option[Order] = (order, payment) => if (payment.status == "PAID") Some(order) else None
	// join streams
	val orderPaid: KStream[OrderId, Order] = ordersStream.join(paymentsStream)(joinOrderPaymentsF, joinWindow).flatMapValues(_.toSeq)
	
	// sink
	userOrderStream.print(Printed.toFile("D:\\test.txt"))
	
	val topology: Topology = builder.build()
	
	val props = new Properties()
	props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

	val app = new KafkaStreams(topology, props)
	app.start()
}
