package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{Heuristic, HeuristicResult, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import org.apache.log4j.Logger

/**
  * Created by mazhiyong on 17/12/11.
  */
class SparkGenericMemoryHeuristic (private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {
  import SparkGenericMemoryHeuristic._

  private var storageMemRatioLimits = Array(0.6d, 0.5d, 0.4d, 0.3d)
  private var _heuristicConfData = null

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    val result = new HeuristicResult(heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0)
    result
  }

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData


}

object SparkGenericMemoryHeuristic{
  private val logger: Logger = Logger.getLogger(classOf[SparkGenericMemoryHeuristic])
  private val STORAGE_MEM_RATIO_SEVERITY = "storage_memory_ratio_severity"

  class Evaluator(sparkGenericMemoryHeuristic: SparkGenericMemoryHeuristic, data: SparkApplicationData) {
    lazy val severity: Severity = Severity.NONE
  }
}