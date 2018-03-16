package com.linkedin.drelephant.spark.heuristics

import java.util.Arrays
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters
import com.linkedin.drelephant.analysis.{Severity, _}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import com.linkedin.drelephant.util._
import com.linkedin.drelephant.math._
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.status.api.v1.StageStatus

/**
  * Created by mazhiyong on 18/2/26.
  */
class StageSkewHeuristic (private val heuristicConfigurationData: HeuristicConfigurationData)
  extends Heuristic[SparkApplicationData] {

  import StageSkewHeuristic._
  import JavaConverters._

  protected lazy val sparkUtils: SparkUtils = SparkUtils

  loadParameters

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
      new HeuristicResultDetails("Stage total", evaluator.stageSkewList.size.toString)
    )

    evaluator.stageSkewList.filter(_.severity.getValue > Severity.LOW.getValue)
      .foreach(stageSkewSummaryInfo  => {
        val stageId = "Stage " + stageSkewSummaryInfo.stageId
        val timeSkewSummary = stageSkewSummaryInfo.timeSkewSummary.get
        var details = Seq(
          new HeuristicResultDetails(stageId , stageSkewSummaryInfo.stageName + " [" + stageSkewSummaryInfo.severity.toString+"]"),
          new HeuristicResultDetails(stageId + " / Gc ratio high count",stageSkewSummaryInfo.stageGCSummary.get.severityACount.toString),
          new HeuristicResultDetails(stageId + " / Gc ratio low count",stageSkewSummaryInfo.stageGCSummary.get.serverityDCount.toString),
          new HeuristicResultDetails(stageId + " / Gc count",stageSkewSummaryInfo.stageGCSummary.get.total.toString),
          new HeuristicResultDetails(stageId + " / " + timeSkewSummary.skewName, timeSkewSummary.severity.toString),
          new HeuristicResultDetails(stageId + " / " + timeSkewSummary.skewName + " Time skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
          new HeuristicResultDetails(stageId + " / " + timeSkewSummary.skewName + " Time skew (Group A)",timeSkewSummary.group1.taskNum + " tasks @ " + sparkUtils.convertTimeMs(timeSkewSummary.group1.avg) + " avg"),
          new HeuristicResultDetails(stageId + " / " + timeSkewSummary.skewName + " Time skew (Group B)",timeSkewSummary.group2.taskNum + " tasks @ " + sparkUtils.convertTimeMs(timeSkewSummary.group2.avg) + " avg")
        )

        if (stageSkewSummaryInfo.inputBytesSkewSummary.nonEmpty) {
          val inputBytesSkewSummary = stageSkewSummaryInfo.inputBytesSkewSummary.get
          if (inputBytesSkewSummary.group1.taskNum > 0 || inputBytesSkewSummary.group2.taskNum >0) {
            val inputBytesSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + inputBytesSkewSummary.skewName,inputBytesSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + inputBytesSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + inputBytesSkewSummary.skewName + " Data skew (Group A)",inputBytesSkewSummary.group1.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(inputBytesSkewSummary.group1.avg) + " avg"),
              new HeuristicResultDetails(stageId + " / " + inputBytesSkewSummary.skewName + " Data skew (Group B)",inputBytesSkewSummary.group2.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(inputBytesSkewSummary.group2.avg) + " avg")
            )
            details = details ++ inputBytesSkewDetails
          }
        }

        if (stageSkewSummaryInfo.inputRecordsSkewSummary.nonEmpty) {
          val recordsSkewSummary = stageSkewSummaryInfo.inputRecordsSkewSummary.get
          if (recordsSkewSummary.group1.taskNum > 0 || recordsSkewSummary.group2.taskNum >0) {
            val recordsSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + recordsSkewSummary.skewName,recordsSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + recordsSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + recordsSkewSummary.skewName + " Data skew (Group A)",recordsSkewSummary.group1.taskNum + " tasks @ " + recordsSkewSummary.group1.avg + " avg"),
              new HeuristicResultDetails(stageId + " / " + recordsSkewSummary.skewName + " Data skew (Group B)",recordsSkewSummary.group2.taskNum + " tasks @ " + recordsSkewSummary.group2.avg + " avg")
            )
            details = details ++ recordsSkewDetails
          }
        }

        if (stageSkewSummaryInfo.shuffleReadBytesSkewSummary.nonEmpty) {
          val shuffleReadBytesSkewSummary = stageSkewSummaryInfo.shuffleReadBytesSkewSummary.get
          if (isDisplayItem(shuffleReadBytesSkewSummary.group1.taskNum, shuffleReadBytesSkewSummary.group2.taskNum,shuffleReadBytesSkewSummary.group1.avg, shuffleReadBytesSkewSummary.group2.avg)) {
            val shuffleReadBytesSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + shuffleReadBytesSkewSummary.skewName,shuffleReadBytesSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleReadBytesSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleReadBytesSkewSummary.skewName + " Data skew (Group A)",shuffleReadBytesSkewSummary.group1.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(shuffleReadBytesSkewSummary.group1.avg) + " avg"),
              new HeuristicResultDetails(stageId + " / " + shuffleReadBytesSkewSummary.skewName + " Data skew (Group B)",shuffleReadBytesSkewSummary.group2.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(shuffleReadBytesSkewSummary.group2.avg) + " avg")
            )
            details = details ++ shuffleReadBytesSkewDetails
          }
        }

        if (stageSkewSummaryInfo.shuffleReadRecordsSkewSummary.nonEmpty) {
          val shuffleReadRecordsSkewSummary = stageSkewSummaryInfo.shuffleReadRecordsSkewSummary.get
          if (isDisplayItem(shuffleReadRecordsSkewSummary.group1.taskNum, shuffleReadRecordsSkewSummary.group2.taskNum,shuffleReadRecordsSkewSummary.group1.avg, shuffleReadRecordsSkewSummary.group2.avg)) {
            val shuffleReadRecordsSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + shuffleReadRecordsSkewSummary.skewName,shuffleReadRecordsSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleReadRecordsSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleReadRecordsSkewSummary.skewName + " Data skew (Group A)",shuffleReadRecordsSkewSummary.group1.taskNum + " tasks @ " + shuffleReadRecordsSkewSummary.group1.avg + " avg"),
              new HeuristicResultDetails(stageId + " / " + shuffleReadRecordsSkewSummary.skewName + " Data skew (Group B)",shuffleReadRecordsSkewSummary.group2.taskNum + " tasks @ " + shuffleReadRecordsSkewSummary.group2.avg + " avg")
            )
            details = details ++ shuffleReadRecordsSkewDetails
          }
        }

        if (stageSkewSummaryInfo.shuffleWriteBytesSkewSummary.nonEmpty) {
          val shuffleWriteBytesSkewSummary = stageSkewSummaryInfo.shuffleWriteBytesSkewSummary.get
          if (isDisplayItem(shuffleWriteBytesSkewSummary.group1.taskNum, shuffleWriteBytesSkewSummary.group2.taskNum,shuffleWriteBytesSkewSummary.group1.avg, shuffleWriteBytesSkewSummary.group2.avg)) {
            val shuffleWriteBytesSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + shuffleWriteBytesSkewSummary.skewName,shuffleWriteBytesSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteBytesSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteBytesSkewSummary.skewName + " Data skew (Group A)",shuffleWriteBytesSkewSummary.group1.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(shuffleWriteBytesSkewSummary.group1.avg) + " avg"),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteBytesSkewSummary.skewName + " Data skew (Group B)",shuffleWriteBytesSkewSummary.group2.taskNum + " tasks @ " + FileUtils.byteCountToDisplaySize(shuffleWriteBytesSkewSummary.group2.avg) + " avg")
            )
            details = details ++ shuffleWriteBytesSkewDetails
          }
        }

        if (stageSkewSummaryInfo.shuffleWriteRecordsSkewSummary.nonEmpty) {
          val shuffleWriteRecordsSkewSummary = stageSkewSummaryInfo.shuffleWriteRecordsSkewSummary.get
          if (isDisplayItem(shuffleWriteRecordsSkewSummary.group1.taskNum, shuffleWriteRecordsSkewSummary.group2.taskNum,shuffleWriteRecordsSkewSummary.group1.avg, shuffleWriteRecordsSkewSummary.group2.avg)) {
            val shuffleWriteRecordsSkewDetails = Seq(
              new HeuristicResultDetails(stageId + " / " + shuffleWriteRecordsSkewSummary.skewName,shuffleWriteRecordsSkewSummary.severity.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteRecordsSkewSummary.skewName + " Data skew (Number of tasks)",stageSkewSummaryInfo.taskTotal.toString),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteRecordsSkewSummary.skewName + " Data skew (Group A)",shuffleWriteRecordsSkewSummary.group1.taskNum + " tasks @ " + shuffleWriteRecordsSkewSummary.group1.avg + " avg"),
              new HeuristicResultDetails(stageId + " / " + shuffleWriteRecordsSkewSummary.skewName + " Data skew (Group B)",shuffleWriteRecordsSkewSummary.group2.taskNum + " tasks @ " + shuffleWriteRecordsSkewSummary.group2.avg + " avg")
            )
            details = details ++ shuffleWriteRecordsSkewDetails
          }
        }

        resultDetails = resultDetails ++ details
      })

    val result = new HeuristicResult(
      heuristicConfigurationData.getClassName,
      heuristicConfigurationData.getHeuristicName,
      evaluator.stageSkewSeverity,
      0,
      resultDetails.asJava
    )
    result
  }

  def isDisplayItem(taskNum1:Long, taskNum2:Long, avg1:Double, avg2:Double): Boolean = {
    if (avg1 == 0 && avg2 == 0) return false
    if (taskNum1 == 0 && taskNum2 ==0) return false
    true
  }

  def loadParameters {
    val paramMap = heuristicConfigurationData.getParamMap
    val heuristicName = heuristicConfigurationData.getHeuristicName

    val confNumTasksThreshold = Utils.getParam(paramMap.get(NUM_TASKS_SEVERITY), numTasksLimits.length)
    if (confNumTasksThreshold != null) numTasksLimits = confNumTasksThreshold
    logger.info(heuristicName + " will use " + NUM_TASKS_SEVERITY + " with the following threshold settings: " + Arrays.toString(numTasksLimits))

    val confDeviationThreshold = Utils.getParam(paramMap.get(DEVIATION_SEVERITY), deviationLimits.length)
    if (confDeviationThreshold != null) deviationLimits = confDeviationThreshold
    logger.info(heuristicName + " will use " + DEVIATION_SEVERITY + " with the following threshold settings: " + Arrays.toString(deviationLimits))

    val confFilesThreshold = Utils.getParam(paramMap.get(FILES_SEVERITY), filesLimits.length)
    if (confFilesThreshold != null) filesLimits = confFilesThreshold
    logger.info(heuristicName + " will use " + FILES_SEVERITY + " with the following threshold settings: " + Arrays.toString(filesLimits))

    for (i <- Range(0, filesLimits.length)) {
      filesLimits(i) = filesLimits(i) * HDFSContext.HDFS_BLOCK_SIZE
    }
  }
}

case class StageGCSummary(severityACount : Int, serverityDCount : Int, total : Int)
case class StageSkewGroupValue(taskNum:Long, avg:Long)
case class StageSkewSummaryDetail(skewName:String, group1:StageSkewGroupValue, group2:StageSkewGroupValue, severity:Severity)
case class StageSkewSummaryInfo(stageName:String, stageId:Int, taskTotal:Long,
                                inputBytesSkewSummary:Option[StageSkewSummaryDetail] = None, inputRecordsSkewSummary:Option[StageSkewSummaryDetail] = None,
                                shuffleReadBytesSkewSummary:Option[StageSkewSummaryDetail] = None, shuffleReadRecordsSkewSummary:Option[StageSkewSummaryDetail] = None,
                                shuffleWriteBytesSkewSummary:Option[StageSkewSummaryDetail] = None, shuffleWriteRecordsSkewSummary:Option[StageSkewSummaryDetail] = None,
                                timeSkewSummary:Option[StageSkewSummaryDetail] = None,
                                stageGCSummary:Option[StageGCSummary]= None,
                                severity:Severity)

object StageSkewHeuristic {
  val logger = Logger.getLogger(classOf[StageSkewHeuristic])

  // Severity Parameters
  val NUM_TASKS_SEVERITY = "num_tasks_severity"
  val DEVIATION_SEVERITY = "deviation_severity"
  val FILES_SEVERITY = "files_severity"

  // Default value of parameters
  var numTasksLimits : Array[Double] = Array(10, 50, 100, 200) // Number of map or reduce tasks
  var deviationLimits : Array[Double] = Array(2, 4, 8, 16) // Deviation in i/p bytes btw 2 groups
  var filesLimits : Array[Double] = Array(1d / 8, 1d / 4, 1d / 2, 1d) // Fraction of HDFS Block Size

  /** The ascending severity thresholds for the ratio of JVM GC Time and Task Run Time (checking whether ratio is above normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_A_THRESHOLDS =
    SeverityThresholds(low = 0.08D, moderate = 0.1D, severe = 0.15D, critical = 0.2D, ascending = true)

  /** The descending severity thresholds for the ratio of JVM GC Time and Task Run Time (checking whether ratio is below normal)
    * These thresholds are experimental and are likely to change */
  val DEFAULT_GC_SEVERITY_D_THRESHOLDS =
    SeverityThresholds(low = 0.05D, moderate = 0.04D, severe = 0.03D, critical = 0.01D, ascending = false)

  val GC_SEVERITY_A_THRESHOLDS_KEY: String = "gc_severity_A_threshold"
  val GC_SEVERITY_D_THRESHOLDS_KEY: String = "gc_severity_D_threshold"


  class Evaluator(stageSkewHeuristic: StageSkewHeuristic, data: SparkApplicationData) {
    lazy val stageSkewList = {
      getStageSkewList(data.stageDatas)
    }

    lazy val stageSkewSeverity = getStageSkewSeverity(stageSkewList)

    def getGcRatiosSummary(gcRatios:Seq[Double]): StageGCSummary = {
      var totalSeverityA : Int = 0
      var totalSeverityD : Int = 0

      gcRatios.foreach(gcRatio => {
        val severityA : Severity = stageSkewHeuristic.gcSeverityAThresholds.severityOf(gcRatio)
        val severityD : Severity = stageSkewHeuristic.gcSeverityDThresholds.severityOf(gcRatio)
        /** The task is spending too much time on GC. We recommend increasing the executor memory. */
        if (severityA.getValue > Severity.LOW.getValue) {
          totalSeverityA += 1
        }
        /** The task is spending too less time in GC. Please check if you have asked for more executor memory than required. */
        if (severityD.getValue > Severity.LOW.getValue) {
          totalSeverityD += 1
        }
      })
      new StageGCSummary(totalSeverityA, totalSeverityD,gcRatios.size)
    }

    def getStageSkewSeverity(stageSkewList:Seq[StageSkewSummaryInfo]) : Severity = {
      var severity = Severity.LOW
      stageSkewList.foreach(stageSkewSummaryInfo => {
        severity = Severity.max(severity,stageSkewSummaryInfo.severity)
      })
      severity
    }

    def getStageSkewList(stageDataList: Seq[StageData]) : Seq[StageSkewSummaryInfo]= {
      logger.info("StageDataList is noEmpty " + stageDataList.nonEmpty)
      var StageSkewSummaryList = Seq[StageSkewSummaryInfo]()
      stageDataList.foreach(stageData => {
        if (stageData.status == StageStatus.COMPLETE) {
          val (inputBytesReads, inputRecordsReads, taskInExecutorRunTimes,shuffleBytesReads, shuffleRecordsReads, shuffleBytesWrites, shuffleRecordsWrites, gcRatios)= getStagTaskInfo(stageData)
          val stageGCSummary = getGcRatiosSummary(gcRatios)
          val timeSkewDetail = getStageTimeSkewSeverityDetail(taskInExecutorRunTimes)

          val inputBytesSkewDetail = getStageDataSkewSeverityDetail(inputBytesReads)
          val shuffleReadBytesSkewDetail = getStageDataSkewSeverityDetail(shuffleBytesReads,"shuffle read bytes")
          val shuffleWriteBytesSkewDetail = getStageDataSkewSeverityDetail(shuffleBytesWrites,"shuffle write bytes")

          val inputRecordsSkewDetail = getStageDataSkewSeverityDetail(inputRecordsReads,"input Records",false)
          val shuffleReadRecordsSkewDetail = getStageDataSkewSeverityDetail(shuffleRecordsReads,"shuffle read records",false)
          val shuffleWriteRecordsSkewDetail = getStageDataSkewSeverityDetail(shuffleRecordsWrites,"shuffle write records",false)

          val severity = Severity.max(inputBytesSkewDetail.severity, inputRecordsSkewDetail.severity,
            timeSkewDetail.severity,
            shuffleReadBytesSkewDetail.severity, shuffleWriteBytesSkewDetail.severity,
            shuffleReadRecordsSkewDetail.severity, shuffleWriteRecordsSkewDetail.severity
          )
          val summary:StageSkewSummaryInfo = new StageSkewSummaryInfo(stageData.name, stageData.stageId, stageData.tasks.get.size,
            Some(inputBytesSkewDetail), Some(inputRecordsSkewDetail),
            Some(shuffleReadBytesSkewDetail), Some(shuffleReadRecordsSkewDetail),
            Some(shuffleWriteBytesSkewDetail), Some(shuffleWriteRecordsSkewDetail),
            Some(timeSkewDetail), Some(stageGCSummary), severity)
          StageSkewSummaryList = StageSkewSummaryList :+ summary
        }
      })
      StageSkewSummaryList
    }

    def getStagTaskInfo(stageData: StageData): (Seq[Long], Seq[Long], Seq[Long], Seq[Long], Seq[Long], Seq[Long], Seq[Long],Seq[Double]) = {
      var inputBytesReads = Seq[Long]()
      var inputRecordsReads = Seq[Long]()

      var shuffleBytesReads = Seq[Long]()
      var shuffleRecordsReads = Seq[Long]()

      var shuffleBytesWrites = Seq[Long]()
      var shuffleRecordsWrites = Seq[Long]()

      var taskInExecutorRunTimes = Seq[Long]()
      var ratios = Seq[Double]()
      var stageTotalJvmGCTime : Long = 0

      require(stageData.tasks.nonEmpty)
      // for each stage task
      val taskDataList = stageData.tasks.get
      taskDataList.values.foreach(taskData => {
        val taskMetrics = taskData.taskMetrics.get

        val taskJvmGcTime = taskMetrics.jvmGcTime
        stageTotalJvmGCTime += taskJvmGcTime

        var ratio: Double = taskJvmGcTime.toDouble / taskMetrics.executorRunTime.toDouble
        ratios = ratios :+ ratio

        taskInExecutorRunTimes = taskInExecutorRunTimes :+ taskMetrics.executorRunTime

        if (taskMetrics.inputMetrics.nonEmpty) {
          val inputMetrics = taskMetrics.inputMetrics.get
          inputRecordsReads = inputRecordsReads :+ inputMetrics.recordsRead
          inputBytesReads = inputBytesReads :+ inputMetrics.bytesRead
        }

        if (taskMetrics.shuffleReadMetrics.nonEmpty) {
          val shuffleReadMetrics = taskMetrics.shuffleReadMetrics.get
          val totalShuffleBytesRead = shuffleReadMetrics.remoteBytesRead + shuffleReadMetrics.localBytesRead
          shuffleRecordsReads = shuffleRecordsReads :+ shuffleReadMetrics.recordsRead
          shuffleBytesReads = shuffleBytesReads :+ totalShuffleBytesRead
        }

        if (taskMetrics.shuffleWriteMetrics.nonEmpty) {
          val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics.get
          shuffleRecordsWrites = shuffleRecordsWrites :+ shuffleWriteMetrics.recordsWritten
          shuffleBytesWrites = shuffleBytesWrites :+ shuffleWriteMetrics.bytesWritten
        }

      })

      (inputBytesReads, inputRecordsReads, taskInExecutorRunTimes, shuffleBytesReads, shuffleRecordsReads, shuffleBytesWrites, shuffleRecordsWrites, ratios)
    }
  }


  def getStageTimeSkewSeverityDetail(timeTaken: Seq[Long]): StageSkewSummaryDetail = {
    val groupsTime = Statistics.findTwoGroups(timeTaken.toArray)
    val timeAvg1 = Statistics.average(groupsTime(0))
    val timeAvg2 = Statistics.average(groupsTime(1))
    //seconds are used for calculating deviation as they provide a better idea than millisecond.
    val timeAvgSec1 = TimeUnit.MILLISECONDS.toSeconds(timeAvg1)
    val timeAvgSec2 = TimeUnit.MILLISECONDS.toSeconds(timeAvg2)
    val minTime = Math.min(timeAvgSec1, timeAvgSec2)
    val diffTime = Math.abs(timeAvgSec1 - timeAvgSec2)
    //using the same deviation limits for time skew as for data skew. It can be changed in the fututre.
    var severityTime = getDeviationSeverity(minTime, diffTime)
    //This reduces severity if number of tasks is insignificant
    severityTime = Severity.min(severityTime, Severity.getSeverityAscending(groupsTime(0).length, numTasksLimits(0), numTasksLimits(1), numTasksLimits(2), numTasksLimits(3)))
    val group1 = new StageSkewGroupValue(groupsTime(0).length,timeAvg1)
    val group2 = new StageSkewGroupValue(groupsTime(1).length,timeAvg2)
    new StageSkewSummaryDetail("time skew", group1,group2,severityTime)
  }

  def getStageDataSkewSeverityDetail(inputBytesOrrecords:Seq[Long], skewName:String ="inputBytes", isReduces:Boolean = true): StageSkewSummaryDetail = {
    //Analyze data. TODO: This is a temp fix. findTwogroups should support list as input
    val groups = Statistics.findTwoGroups(inputBytesOrrecords.toArray)
    val avg1 = Statistics.average(groups(0))
    val avg2 = Statistics.average(groups(1))
    val min = Math.min(avg1, avg2)
    val diff = Math.abs(avg2 - avg1)
    var severityData = getDeviationSeverity(min, diff)
    if (isReduces) {
      //This reduces severity if the largest file sizes are insignificant
      severityData = Severity.min(severityData, getFilesSeverity(avg2))
      //This reduces severity if number of tasks is insignificant
      severityData = Severity.min(severityData, Severity.getSeverityAscending(groups(0).length, numTasksLimits(0), numTasksLimits(1), numTasksLimits(2), numTasksLimits(3)))
    }
    val group1 = new StageSkewGroupValue(groups(0).length,avg1)
    val group2 = new StageSkewGroupValue(groups(1).length,avg2)
    new StageSkewSummaryDetail(skewName, group1,group2,severityData)
  }

  def getDeviationSeverity(avgMin: Long, averageDiff: Long) = {
    var averageMin  = avgMin
    if (averageMin <= 0) averageMin = 1
    val value = averageDiff / averageMin
    Severity.getSeverityAscending(value, deviationLimits(0), deviationLimits(1), deviationLimits(2), deviationLimits(3))
  }

  def getFilesSeverity(value: Long) = Severity.getSeverityAscending(value, filesLimits(0), filesLimits(1), filesLimits(2), filesLimits(3))

}