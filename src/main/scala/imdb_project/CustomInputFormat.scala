package imdb_project

import java.nio.charset.Charset

import org.apache.flink.api.common.io.DelimitedInputFormat

class CustomInputFormat(charsetName: String, lineDelim: String) extends DelimitedInputFormat[String] {

  if(charsetName == null || !Charset.isSupported(charsetName)) {
    throw new RuntimeException("Charset not supported: " + charsetName)
  }
  val mCharsetName = charsetName
  super.setDelimiter(lineDelim)

  // 2ndry constructors
  def this(charsetName: String) {
    this(charsetName, "\n\n")
  }

  override def readRecord(ot: String, bytes: Array[Byte], offset: Int, numBytes: Int): String = {

    var numbytes = numBytes

    if(this.getDelimiter != null      &&
      this.getDelimiter.length == 1  &&
      this.getDelimiter()(0) == 10   &&
      offset + numBytes >= 1         &&
      bytes(offset + numBytes - 1) == 13)
    {
      numbytes -= 1
    }

    new String(bytes, offset, numbytes, this.mCharsetName)
  }
}

