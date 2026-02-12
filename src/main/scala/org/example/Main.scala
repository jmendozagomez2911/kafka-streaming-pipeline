package org.example

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello and welcome!")

    for (i <- 1 to 5) {
      println(s"i = $i")
    }
  }
}