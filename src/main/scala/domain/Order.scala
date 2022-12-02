package com.example
package domain

import domain.Domain._

case class Order(
                  orderId: OrderId
                , userId: UserId
                , products: List[Product]
                , amount: Amount
                )
