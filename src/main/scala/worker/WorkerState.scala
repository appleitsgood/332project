package worker

import common.{PartitionPlan, Record}

object WorkerState {
  @volatile private var sortedRecords: Option[Seq[Record]]   = None
  @volatile private var partitionPlan: Option[PartitionPlan] = None

  def setSortedRecords(recs: Seq[Record]): Unit = synchronized {
    sortedRecords = Some(recs)
  }

  def getSortedRecords: Option[Seq[Record]] = sortedRecords

  def setPartitionPlan(plan: PartitionPlan): Unit = synchronized {
    partitionPlan = Some(plan)
  }

  def getPartitionPlan: Option[PartitionPlan] = partitionPlan
}
