
import org.apache.commons.lang3.RandomStringUtils

import java.io.{BufferedWriter, FileWriter}

object Test {
  def main(args: Array[String]): Unit = {

    val bw = new BufferedWriter(new FileWriter("d:/wc.txt"))
    for(i <-1 to 100000000){
      bw.write(RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
        +" "+ RandomStringUtils.randomAlphabetic(3,6)
      )
      bw.newLine()
    }
    bw.close()

  }
}
