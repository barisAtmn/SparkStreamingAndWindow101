package com.baris.spark101

import java.io.PrintStream
import java.net.ServerSocket

/**
  * Send data to localhost:12345
 **/
object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)
  println("socket accepted")

  def example() = {
    printer.println("7000,blue")
    printer.println("8000,green")
    printer.println("9000,yellow")
    printer.println("6000,orange")
  }

  def main(args: Array[String]): Unit = {
    example()
  }
}
