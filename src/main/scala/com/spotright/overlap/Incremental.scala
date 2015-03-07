package com.spotright.overlap

import com.spotright.common.lib.PairingHeap

object Incremental {

  def log(m: String): Unit = System.err.println(s";; $m")

  case class Opts(base: Seq[String], ladder: Seq[String], counts: Boolean = false)

  object Opts {
    val empty = Opts(Seq.empty[String], Seq.empty[String])
  }

  val optParser = new scopt.OptionParser[Opts]("Incremental") {
    opt[String]('b', "base") required() unbounded() valueName "<file>" text "Files to report against." action {case (v, c) => c.copy(base = c.base :+ v)}
    opt[Unit]("counts") text "Show counts instead of percents." action {case (v, c) => c.copy(counts = true)}
    arg[String]("<file>") unbounded() text "Files forming the ladder." action {case (v, c) => c.copy(ladder = c.ladder :+ v)}
  }

  /**
   * Given some base files and files that will form a ladder report matches against the base files
   * in such a way that hits occurring in files down the ladder are not reported if the hit occurs
   * higher in the ladder.  The idea is that the report will show the matches in the first elt of the
   * ladder while matches in the second elt of the ladder are those that did not also occur in the first,
   * and so on down the ladder.
   */
  def main(av: Array[String]): Unit = {
    val opts = optParser.parse(av, Opts.empty).fold(sys.exit(1))(identity)
    val numf = if (opts.counts) asCount _ else asPercent _

    // The count powerset.  Keys are a set of the ladder indexes that matched.
    val psets = Array.fill(opts.base.length){Map.empty[Set[Int],Long]}

    val base = opts.base.zipWithIndex.map {
      case (fn, idx) =>
        val iter = io.Source.fromFile(fn).getLines().map {_.trim.toLowerCase}.filterNot {_.isEmpty}

        // We only ever count our own lines so `counts` set to size 1
        val fcc = FileCC("-", None, fn, idx, UniqIterator(iter.buffered), Array.fill(1)(0L))

        fcc.next()
    }

    val ladder = opts.ladder.zipWithIndex.map {
      case (fn, idx) =>
        val iter = io.Source.fromFile(fn).getLines().map {_.trim.toLowerCase}.filterNot {_.isEmpty}

        // We won't be keeping a count in this FileCC so `counts` set to size 0
        val fcc = FileCC("-", None, fn, idx, UniqIterator(iter.buffered), Array.fill(0)(0L))

        fcc.next()
    }

    val qbase =
      PairingHeap.empty()(FileCC.HeadOnlyOrdering) ++
        base.filterNot{_.isDone}

    val qladder =
      PairingHeap.empty()(FileCC.HeadOnlyOrdering) ++
        ladder.filterNot{_.isDone}

    while (qbase.nonEmpty) {
      val top = qbase.dequeue()

      // grab other base elts that match
      val toIncr =
        if (qbase.isEmpty) List.empty[FileCC]
        else if (qbase.inspect().head != top.head) List.empty[FileCC]
        else {
          val bldr = List.newBuilder[FileCC]

          while (qbase.nonEmpty && qbase.inspect().head == top.head) {
            bldr += qbase.dequeue()
          }

          bldr.result()
      }

      val tidxs =
        (top :: toIncr).map {
          fcc =>
            // catch and release
            base(fcc.idx).counts(0) += 1
            if (fcc.hasNext) qbase += fcc.next()

            fcc.idx
        }

      // ladder matches
      while (qladder.nonEmpty && qladder.inspect().head < top.head) {
        val qtop = qladder.dequeue()
        if (qtop.hasNext) qladder += qtop.next()
      }

      val matches =
        if (qladder.isEmpty) List.empty[FileCC]
        else if (qladder.inspect().head != top.head) List.empty[FileCC]
        else {
          val bldr = List.newBuilder[FileCC]

          while (qladder.nonEmpty && qladder.inspect().head == top.head) {
            bldr += qladder.dequeue()
          }

          bldr.result()
        }

      if (matches.nonEmpty) {
        val key = matches.foldLeft(Set.empty[Int]){case (s, fcc) => s + fcc.idx}

        tidxs.foreach {
          idx =>
            psets(idx) += key -> (psets(idx).getOrElse(key, 0L) + 1)
        }

        // catch and release
        matches.foreach {
          fcc =>
            if (fcc.hasNext) qladder += fcc.next()
        }
      }
    }

    // *** At this point we should have all the data.

    // -            file1        file2
    // -             size         size
    // -
    // ladder1      match%       match%
    // ladder2      submatch% submatch%

    // We'll emit as a CSV to suck into google sheets

    println(s",${base.map{_.name}.mkString(",")}")
    println(s",${base.map{_.counts(0)}.mkString(",")}")
    println(s",${base.map{_ => ""}.mkString(",")}")

    val totals = Array.fill(opts.base.length)(0L)

    ladder.foreach {
      fcc =>
        // ladder count at this run against the proper base file as a percent of base files size
        val lms = psets.zipWithIndex.map {
          case (pset, idx) =>
            val lm = ladderMatch(fcc.idx, pset)
            totals(idx) += lm

            numf(lm, base(idx).counts(0))
        }

        println(s"${fcc.name},${lms.mkString(",")}")
    }

    println(s",${base.map{_ => ""}.mkString(",")}")
    println(f"total,${numf(totals(0), base(0).counts(0))},${numf(totals(1), base(1).counts(0))}")
  }

  def asPercent(n: Long, d: Long): String = (n.toDouble / d * 100.0).formatted("%.2f%%")

  def asCount(n: Long, d: Long): String = n.toString

  def isSubset[A: Ordering](a: Set[A], b: Set[A]): Boolean = a.intersect(b) == b

  def ladderMatch(idx: Int, pset: Map[Set[Int],Long]): Long = {
    val target = Set(idx)
    val masks = (0 until idx).map{x => Set(x)}

    pset.filter{
      case (k,_) =>
        isSubset(k, target) && !masks.exists{m => isSubset(k, m)}
    }.values.sum
  }
}
