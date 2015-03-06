package com.comcast.xfinity.sirius.uberstore.data

import java.io.RandomAccessFile
import java.nio.ByteBuffer

trait UberFileSource {
  def seek(offset: Long)

  def getFilePointer: Long

  def length: Long

  def readBuffer(size: Integer) : ByteBuffer

  def read(bytes: Array[Byte])

  def close()
}

class CachedFileSource(byteBuffer: ByteBuffer) extends UberFileSource {

  var fileOffset = 0L
  val size = byteBuffer.remaining()

  def seek(offset: Long) = {
    1L to (offset-fileOffset) foreach { _ => byteBuffer.get() }
    fileOffset = offset
  }

  def getFilePointer: Long = fileOffset

  def length: Long = size

  def readBuffer(size: Integer) : ByteBuffer = {
    val cloneBuffer = byteBuffer.slice()
    seek(size.toLong)
    cloneBuffer
  }

  def read(bytes: Array[Byte]) = {
    byteBuffer.get(bytes)
  }

  def close() = {
  }
}

class RandomAccessFileSource(readHandle: RandomAccessFile) extends UberFileSource {

  def seek(offset: Long) = {
    readHandle.seek(offset)
  }

  def getFilePointer: Long = {
    readHandle.getFilePointer
  }

  def length: Long = {
    readHandle.length
  }

  def readBuffer(size: Integer) : ByteBuffer = {
    val entryHeaderBuf = ByteBuffer.allocate(size)
    read(entryHeaderBuf.array)
    entryHeaderBuf
  }

  def read(bytes: Array[Byte]) = {
    readHandle.readFully(bytes)
  }

  def close() = {
    readHandle.close()
  }
}
