package sorting

import common.Record
import common.RecordStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Sampler {

  private final case class ReservoirState(capacity: Int, reservoir: ArrayBuffer[Record], var seen: Long, random: Random)

  private def initReservoir(targetSamples: Int): ReservoirState = {
    val capacity  = targetSamples
    val reservoir = new ArrayBuffer[Record](capacity)
    val seen      = 0L
    val random    = new Random()
    ReservoirState(capacity, reservoir, seen, random)
  }

  private def offerSample(state: ReservoirState, record: Record): Unit = {
    if (state.seen < state.capacity) {
      state.reservoir += record
    } else {
      val index = state.random.nextLong(state.seen + 1)
      if (index < state.capacity) state.reservoir(index.toInt) = record
    }
    state.seen += 1
  }

  def takeUniformSamples(records: Iterable[Record], targetSamples: Int): Vector[Record] = {
    if (targetSamples <= 0) return Vector.empty

    val state = initReservoir(targetSamples)

    records.foreach { record =>
      offerSample(state, record)
    }

    state.reservoir.toVector
  }

  def sampleFromFiles(paths: Seq[String], blockSize: Int, targetSamples: Int): Vector[Record] = {
    if (targetSamples <= 0) return Vector.empty

    val state = initReservoir(targetSamples)

    RecordStream.forEachRecord(paths, blockSize, warnOnPartial = false) { record =>
      offerSample(state, record)
    }

    state.reservoir.toVector
  }

  def sampleFromFile(path: String, blockSize: Int, targetSamples: Int): Vector[Record] =
    sampleFromFiles(Seq(path), blockSize, targetSamples)
}
