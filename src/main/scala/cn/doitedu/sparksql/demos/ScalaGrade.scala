package cn.doitedu.sparksql.demos

import scala.beans.BeanProperty

class ScalaGrade(
                  @BeanProperty
                  val level: Int,
                  @BeanProperty
                  val stuCount: Int,
                  @BeanProperty
                  val teacher: String
                ) extends Serializable
