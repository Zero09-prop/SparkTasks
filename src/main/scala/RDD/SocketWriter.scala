package RDD

import java.io.DataOutputStream
import java.net.Socket
import scala.io.Source

object SocketWriter {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("text.txt")
    val str = source.getLines().toArray
    source.close()
    val socket = new Socket("localhost", 8080)
    val outputStream = socket.getOutputStream
    val dataOutput = new DataOutputStream(outputStream)
    Thread.sleep(2 * 1000)
    str.foreach { string =>
      dataOutput.writeUTF(string)
      Thread.sleep(2 * 1000)
    }
    dataOutput.flush()
    socket.close()
  }
}
