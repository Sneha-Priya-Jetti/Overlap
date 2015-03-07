package com.spotright.overlap

case class UniqIterator[A](iter: BufferedIterator[A]) extends Iterator[A] {
  def hasNext: Boolean = iter.hasNext

  def next(): A = {
    var item = iter.head

    while (iter.hasNext && iter.head == item) {
      item = iter.next()
    }

    item
  }
}
