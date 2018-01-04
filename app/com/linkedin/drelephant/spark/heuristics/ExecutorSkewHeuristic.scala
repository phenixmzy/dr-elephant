package com.linkedin.drelephant.spark.heuristics

import java.util
import java.util.concurrent.TimeUnit

import com.google.common.primitives.Longs
import com.linkedin.drelephant.analysis.{Heuristic, HeuristicResult, HeuristicResultDetails, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.SparkApplicationData
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.collection.JavaConverters

/**
  * Created by mazhiyong on 18/1/3.
  */
class ExecutorSkewHeuristic (private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {
  import ExecutorSkewHeuristic._
  import JavaConverters._

  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    def loadParameters(): Unit = {
      val paramMap = heuristicConfigurationData.getParamMap
      val heuristicName = heuristicConfigurationData.getHeuristicName
    }

    loadParameters
    val evaluator = new Evaluator(this, data)

    val resultDetails = Seq(
      new HeuristicResultDetails("Data skew (Number of Executors)", evaluator.executorNum.toString),
      new HeuristicResultDetails("Data skew (Group A)", evaluator.inputBytesEvaluatorData.data1.executorNum + " tasks @ " + FileUtils.byteCountToDisplaySize(evaluator.inputBytesEvaluatorData.data1.value) + " avg "),
      new HeuristicResultDetails("Data skew (Group B)", evaluator.inputBytesEvaluatorData.data2.executorNum +" tasks @ " + FileUtils.byteCountToDisplaySize(evaluator.inputBytesEvaluatorData.data2.value) + " avg "),

      new HeuristicResultDetails("Time skew (Number of Executors)", evaluator.executorNum.toString),
      new HeuristicResultDetails("Time skew (Group A)", evaluator.timeEvaluatorData.data1.executorNum + " tasks @ " + evaluator.timeEvaluatorData.data1.value + " avg "),
      new HeuristicResultDetails("Time skew (Group B)", evaluator.timeEvaluatorData.data2.executorNum + " tasks @ " + evaluator.timeEvaluatorData.data2.value + " avg ")
    )

    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.severity,
      0,
      resultDetails.asJava
    )
    result
  }
}

object ExecutorSkewHeuristic {
  private val logger: Logger = Logger.getLogger(classOf[ExecutorSkewHeuristic])
  // Severity Parameters
  val NUM_TASKS_SEVERITY = "num_tasks_severity"
  val DEVIATION_SEVERITY = "deviation_severity"
  val FILES_SEVERITY = "files_severity"

  // Default value of parameters
  var numTasksLimits = Array(10, 50, 100, 200) // Number of map or reduce tasks
  var deviationLimits = Array(2, 4, 8, 16) // Deviation in i/p bytes btw 2 groups
  var filesLimits = Array(1d / 8, 1d / 4, 1d / 2, 1d)

  case class GroupEvaluatorData(executorNum : Long, value : Long)
  case class EvaluatorData (data1 : GroupEvaluatorData, data2 : GroupEvaluatorData, severity : Severity)

  // Fraction of HDFS Block Size
  class Evaluator(executorsHeuristic: ExecutorSkewHeuristic, data: SparkApplicationData) {
    lazy val executorNum = data.executorSummaries.size
    lazy val executorTotalDuration = data.executorSummaries.map(_.totalDuration).sum
    lazy val executorTotalInputBytes = data.executorSummaries.map(_.totalInputBytes).sum

    lazy val timeEvaluatorData = getTimeEvaluator(data)
    lazy val inputBytesEvaluatorData = getInputBytesEvaluator(data)

    lazy val severity = Severity.max(inputBytesEvaluatorData.severity, timeEvaluatorData.severity)

    def getTimeEvaluator(data: SparkApplicationData) : EvaluatorData = {
      //Gathering data for checking time skew
      val timeTaken = data.executorSummaries.map(_.totalDuration).toArray
      val groupsTime: Array[Array[Long]] = Statistics.findTwoGroups(timeTaken)
      val timeAvg1: Long = Statistics.average(groupsTime(0))
      val timeAvg2: Long = Statistics.average(groupsTime(1))

      //seconds are used for calculating deviation as they provide a better idea than millisecond.
      val timeAvgSec1: Long = TimeUnit.MILLISECONDS.toSeconds(timeAvg1)
      val timeAvgSec2: Long = TimeUnit.MILLISECONDS.toSeconds(timeAvg2)
      val minTime: Long = Math.min(timeAvgSec1, timeAvgSec2)
      val diffTime: Long = Math.abs(timeAvgSec1 - timeAvgSec2)
      val severity = Severity.min(getDeviationSeverity(minTime, diffTime), Severity.getSeverityAscending(groupsTime(0).length, numTasksLimits(0), numTasksLimits(1), numTasksLimits(2), numTasksLimits(3)))
      new EvaluatorData(new GroupEvaluatorData(groupsTime(0).length,timeAvg1), new GroupEvaluatorData(groupsTime(1).length,timeAvg2), severity)

    }

    def getInputBytesEvaluator(data: SparkApplicationData) : EvaluatorData = {
      val executorNum = data.executorSummaries.size
      val inputBytes = data.executorSummaries.map(_.totalInputBytes).toArray

      // Ratio of total tasks / sampled tasks
      val scale = executorNum.toDouble / inputBytes.size

      //Analyze data. TODO: This is a temp fix. findTwogroups should support list as input
      val groups = Statistics.findTwoGroups(inputBytes)
      val avg1 = Statistics.average(groups(0))
      val avg2 = Statistics.average(groups(1))
      val min = Math.min(avg1, avg2)
      val diff = Math.abs(avg2 - avg1)
      var severityData = Severity.min(getDeviationSeverity(min, diff), getFilesSeverity(avg2))
      val severity = Severity.min(severityData, Severity.getSeverityAscending(groups(0).length, numTasksLimits(0), numTasksLimits(1), numTasksLimits(2), numTasksLimits(3)))
      new EvaluatorData(new GroupEvaluatorData(groups(0).length,avg1), new GroupEvaluatorData(groups(1).length,avg2), severity)

    }

    def getDeviationSeverity (averageMin: Long, averageDiff: Long): Severity = {
      var avgMin = averageMin
      if (avgMin < 0) avgMin =1
      val value = averageDiff / avgMin
      Severity.getSeverityAscending(value, deviationLimits(0), deviationLimits(1), deviationLimits(2), deviationLimits(3))
    }

    def getFilesSeverity (value: Long): Severity = {
      return Severity.getSeverityAscending(value, filesLimits(0), filesLimits(1), filesLimits(2), filesLimits(3))
    }
  }
}
