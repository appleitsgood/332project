package common

import sorting.Pivoter

final case class PartitionPlan(pivots: Vector[Record]) {
  def numPartitions: Int = pivots.length + 1
}

object PartitionPlan {

  def fromPivots(pivots: Vector[Record]): PartitionPlan =
    PartitionPlan(pivots)

  def toPivots(plan: PartitionPlan): Vector[Record] =
    plan.pivots
}
