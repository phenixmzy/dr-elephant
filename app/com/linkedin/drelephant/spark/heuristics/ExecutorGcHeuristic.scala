package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.Severity
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import com.linkedin.drelephant.analysis._
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.SparkUtils

import scala.collection.JavaConverters
/**
  * Created by mazhiyong on 18/2/8.
  */
class ExecutorGcHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import ExecutorGcHeuristic._
  import JavaConverters._

  protected lazy val sparkUtils: SparkUtils = SparkUtils

  val gcSeverityAThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(GC_SEVERITY_A_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_GC_SEVERITY_A_THRESHOLDS)

  val gcSeverityDThresholds: SeverityThresholds =
    SeverityThresholds.parse(heuristicConfigurationData.getParamMap.get(GC_SEVERITY_D_THRESHOLDS_KEY), ascending = true)
      .getOrElse(DEFAULT_GC_SEVERITY_D_THRESHOLDS)

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("GC time to Executor Run time ratio", evaluator.ratio.toString),
      new HeuristicResultDetails("Total GC time", sparkUtils.convertTimeMs(evaluator.jvmTime)),
      new HeuristicResultDetails("Total Executor Runtime", sparkUtils.convertTimeMs(evaluator.executorRunTimeTotal))
    )

    //adding recommendations to the result, severityTimeA corresponds to the ascending severity calculation
    if (evaluator.severityTimeA.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Gc ratio high", "The job is spending too much time on GC. We recommend increasing the executor memory.")
    }
    //severityTimeD corresponds to the descending severity calculation
    if (evaluator.severityTimeD.getValue > Severity.LOW.getValue) {
      resultDetails = resultDetails :+ new HeuristicResultDetails("Gc ratio low", "The job is spending too less time in GC. Please check if you have asked for more executor memory than required.")
    }

    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severityTimeA,
      0,
      resultDetails.asJava
    )
    result
  }
}

object ExecutorGcHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"

  /** The ascending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is above normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)

  /** The descending severity thresholds for the ratio of JVM GC Time and executor Run Time (checking whether ratio is below normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_D_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.04D, severe = 0.03D, critical = 0.01D, ascending = false)

  val GC_SEVERITY_A_THRESHOLDS_KEY: String = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY: String = "gc_severity_D_threshold"

  class Evaluator(executorGcHeuristic: ExecutorGcHeuristic, data: SparkApplicationData) {
    lazy val executorAndDriverSummaries: Seq[ExecutorSummary] = data.executorSummaries
    lazy val executorSummaries: Seq[ExecutorSummary] = executorAndDriverSummaries.filterNot(_.id.equals("driver"))
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties
    var (jvmTime, executorRunTimeTotal) = getTimeValues(executorSummaries)

    var ratio: Double = jvmTime.toDouble / executorRunTimeTotal.toDouble

    lazy val severityTimeA: Severity = executorGcHeuristic.gcSeverityAThresholds.severityOf(ratio)
    lazy val severityTimeD: Severity = executorGcHeuristic.gcSeverityDThresholds.severityOf(ratio)

    /**
      * returns the total JVM GC Time and total executor Run Time across all stages
      * @param executorSummaries
      * @return
      */
    private def getTimeValues(executorSummaries: Seq[ExecutorSummary]): (Long, Long) = {
      var jvmGcTimeTotal: Long = 0
      var executorRunTimeTotal: Long = 0
      executorSummaries.foreach(executorSummary => {
        jvmGcTimeTotal+=executorSummary.totalGCTime
        executorRunTimeTotal+=executorSummary.totalDuration
      })
      (jvmGcTimeTotal, executorRunTimeTotal)
    }
  }
}
