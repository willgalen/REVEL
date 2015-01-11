package com.datastax.driver.scala.core.io

/** Represents a logical conjunction of CQL predicates.
  * Each predicate can have placeholders denoted by '?' which get substituted by values from the `values` array.
  * The number of placeholders must match the size of the `values` array. */
case class CqlWhereClause(predicates: Seq[String], values: Seq[Any]) {

  /** Returns a conjunction of this clause and the given predicate. */
  def and(other: CqlWhereClause) =
    CqlWhereClause(predicates ++ other.predicates, values ++ other.values)

}

object CqlWhereClause {

  /** Empty CQL WHERE clause selects all rows */
  val empty = new CqlWhereClause(Nil, Nil)
}

/** Stores a CQL `WHERE` predicate matching a range of tokens. */
case class CqlTokenRange(cql: String, values: Any*)

