package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import scala.annotation.tailrec

import com.datastax.spark.connector.rdd.partitioner.dht.{CassandraNode, Token, TokenRange}

class MergingTokenRangeClusterer[V, T <: Token[V]](maxRowCountPerGroup: Long, maxGroupSize: Int = Int.MaxValue) {

  private implicit object InetAddressOrdering extends Ordering[InetAddress] {
    override def compare(x: InetAddress, y: InetAddress) =
      x.getHostAddress.compareTo(y.getHostAddress)
  }

  @tailrec
  private def mergeAdjacent(tokenRanges: Stream[TokenRange[V, T]], result: Vector[TokenRange[V, T]]): Vector[TokenRange[V, T]] = {
    tokenRanges match {
      case Stream.Empty => result
      case head #:: rest =>
        val endpoints = head.endpoints
        val rowCounts = tokenRanges.map(_.rowCount.get)
        val cumulativeRowCounts = rowCounts.scanLeft(0L)(_ + _).tail // drop first item always == 0
      val commonEndpoints = tokenRanges.map(_.endpoints).scanLeft(endpoints)(_ intersect _).tail
        val rowLimit = math.max(maxRowCountPerGroup / 2, head.rowCount.get) // make sure first element will be always included
      val cluster = tokenRanges
          .zip(commonEndpoints)
          .zip(cumulativeRowCounts)
          .takeWhile { case ((tr, ep), count) => count <= rowLimit && ep.nonEmpty }
          .toVector

        val remainingTokenRanges = tokenRanges.drop(cluster.length)
        mergeAdjacent(remainingTokenRanges, result :+ cluster.head._1._1.copy[V, T](
          end = cluster.last._1._1.end,
          endpoints = cluster.last._1._2,
          rowCount = Some(cluster.last._2)))
    }

  }

  @tailrec
  private def group(tokenRanges: Stream[TokenRange[V, T]],
    result: Vector[Seq[TokenRange[V, T]]]): Iterable[Seq[TokenRange[V, T]]] = {
    tokenRanges match {
      case Stream.Empty => result
      case head #:: rest =>
        val firstEndpoint = head.endpoints.min
        val rowCounts = tokenRanges.map(_.rowCount.get)
        val cumulativeRowCounts = rowCounts.scanLeft(0L)(_ + _).tail // drop first item always == 0
      val rowLimit = math.max(maxRowCountPerGroup, head.rowCount.get) // make sure first element will be always included
      val cluster = tokenRanges
          .take(math.max(1, maxGroupSize))
          .zip(cumulativeRowCounts)
          .takeWhile { case (tr, count) => count <= rowLimit && tr.endpoints.min == firstEndpoint }
          .map(_._1)
          .toVector
        val remainingTokenRanges = tokenRanges.drop(cluster.length)
        group(remainingTokenRanges, result :+ cluster)
    }
  }


  @tailrec
  private def makeGroup(curGroup: List[TokenRange[V, T]], curGroupLength: Long, remaining: List[TokenRange[V, T]], maxSize: Int, maxLength: Long): List[TokenRange[V, T]] = {
    remaining.headOption match {
      case Some(first) =>
        if (curGroup.nonEmpty) {
          if (curGroup.size < maxSize) {
            val addableRanges = remaining.dropWhile {
              case TokenRange(_, _, _, Some(rowCount)) => (curGroupLength + rowCount) > maxLength
            }

            addableRanges.headOption match {
              case Some(tokenRange) =>
                makeGroup(curGroup :+ tokenRange, curGroupLength + tokenRange.rowCount.get, addableRanges.tail, maxSize, maxLength)
              case None => curGroup
            }
          } else {
            curGroup
          }
        } else {
          makeGroup(List(first), first.rowCount.get, remaining.tail, maxSize, maxLength)
        }
      case None =>
        curGroup
    }
  }

  @tailrec
  private def makeGroups(groups: List[List[TokenRange[V, T]]], tokenRanges: List[TokenRange[V, T]], maxSize: Int, maxLength: Long): List[List[TokenRange[V, T]]] = {
    if (tokenRanges.nonEmpty) {
      val group = makeGroup(Nil, 0, tokenRanges, maxSize, maxLength)
      val remaining = tokenRanges diff group
      makeGroups(group :: groups, remaining, maxSize, maxLength)
    } else {
      groups
    }
  }


  /** Groups small token ranges on the same server(s) in order to reduce task scheduling overhead.
    * Useful mostly with virtual nodes, which may create lots of small token range splits.
    * Each group will make a single Spark task. */
  def group(tokenRanges: Seq[TokenRange[V, T]]): Iterable[Seq[TokenRange[V, T]]] = {

    val tokenRangesByEndpoint: Map[CassandraNode, Seq[TokenRange[V, T]]] =
      (for (tokenRange ← tokenRanges; endpoint ← tokenRange.endpoints) yield (endpoint, tokenRange))
        .groupBy(_._1).mapValues(_.map(_._2).sortBy(_.start)(Ordering.ordered[T](x => x.asInstanceOf[Comparable[T]])))





    // sort the token ranges by range start token
    val rangesSortedByToken: Seq[TokenRange[V, T]] = tokenRanges.sortBy(_.start)(Ordering.ordered[T](x => x.asInstanceOf[Comparable[T]]))

    // merge as much adjacent token ranges as possible, given that the merged ranges must have at least
    // a single common endpoint
    val preMergedRanges: Vector[TokenRange[V, T]] = mergeAdjacent(rangesSortedByToken.toStream, Vector.empty)

    // group the merged token ranges by the first endpoint according to the defined InetAddressOrdering
    val rangesGroupedByEndpoint: Map[CassandraNode, Vector[TokenRange[V, T]]] = preMergedRanges.groupBy(_.endpoints.toSeq.sorted.head)

    // convert endpoint to list of token ranges map into a sequence of token range lists, each sorted by row count in descending order
    val rangesInternallySortedByRowCountsDesc: Seq[Vector[TokenRange[V, T]]] = rangesGroupedByEndpoint.toSeq.map(_._2.sortBy(tr => -tr.rowCount.get))

    // for each endpoint related list of token ranges, make groups with respect to the max group size and max row count per group
    rangesInternallySortedByRowCountsDesc.flatMap(tokenRanges => makeGroups(Nil, tokenRanges.toList, maxGroupSize, maxRowCountPerGroup))
  }

}
