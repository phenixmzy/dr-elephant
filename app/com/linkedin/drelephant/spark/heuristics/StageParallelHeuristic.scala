package com.linkedin.drelephant.spark.heuristics

import com.linkedin.drelephant.analysis.{Heuristic, HeuristicResult, HeuristicResultDetails, Severity}
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageData
import com.linkedin.drelephant.util.SparkUtils
import org.apache.log4j.Logger
import org.apache.spark.status.api.v1.StageStatus

import scala.collection.JavaConverters


case class StageParallelSummary(stageId : Int, stageName : String, schedulerDelayRatio : Double, avgSchedulerDelayRatio:Double, avgSchedulerDelay:Double, avgDuration:Double, taskNum : Int, taskLocalitySummary : TaskLocalitySummary)
case class TaskLocalitySummary(val taskLocalitys: Seq[(String,Int)])
/**
  * Created by mazhiyong on 18/3/16.
  */
class StageParallelHeuristic(private val heuristicConfigurationData: HeuristicConfigurationData) extends Heuristic[SparkApplicationData] {

  import StageParallelHeuristic._

  import JavaConverters._

  protected lazy val sparkUtils: SparkUtils = SparkUtils
  override def getHeuristicConfData(): HeuristicConfigurationData = heuristicConfigurationData

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Stage total", evaluator.stageTaskParallelList.size.toString)
    )

    evaluator.stageTaskParallelList.foreach(summary => {
      val stageId = "Stage "+ summary.stageId.toString
      var builder:StringBuilder = new StringBuilder
      summary.taskLocalitySummary.taskLocalitys.foreach(info => builder.append(String.format("%s: %s",info._1,info._2.toString)+"  "))

      var details = Seq(
        new HeuristicResultDetails(stageId , summary.stageName),
        new HeuristicResultDetails(stageId + " / Scheduler Delay Ratio ",f"${summary.schedulerDelayRatio}%1.3f"),
        new HeuristicResultDetails(stageId + " / avg Scheduler Delay Ratio",f"${summary.avgSchedulerDelayRatio}%1.3f"),
        new HeuristicResultDetails(stageId + " / avg Duration",sparkUtils.convertTimeMs(summary.avgDuration.toLong)),
        new HeuristicResultDetails(stageId + " / avg Scheduler Delay" , sparkUtils.convertTimeMs(summary.avgSchedulerDelay.toLong)),
        new HeuristicResultDetails(stageId + " / task Num" , summary.taskNum.toString),
        new HeuristicResultDetails(stageId + " / task locality" , builder.toString)
      )
      resultDetails = resultDetails ++ details
    })

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

object StageParallelHeuristic{
  val logger = Logger.getLogger(classOf[StageParallelHeuristic])

  class Evaluator(stageTaskParallelHeuristic: StageParallelHeuristic, data: SparkApplicationData) {
    lazy val stageTaskParallelList  = getStageTaskParallelList(data.stageDatas)
    lazy val severity = Severity.LOW

    def getStageTaskParallelList(stageDataList: Seq[StageData]): Seq[StageParallelSummary] = {
      var StageParallelSummaryList = Seq[StageParallelSummary]()

      stageDataList.foreach(stageData => {
        if (stageData.status == StageStatus.COMPLETE) {
          var seq = Seq[(String,Int)]()
          val taskNum = stageData.tasks.get.size
          val durationSum : Long = stageData.tasks.get.mapValues(taskData => taskData.duration).values.sum
          val schedulerDelaySum : Long = stageData.tasks.get.mapValues(taskData => taskData.taskMetrics.get.schedulerDelay).values.sum
          val ratio : Double = schedulerDelaySum.toDouble / durationSum.toDouble
          val avgSchedulerDelay = schedulerDelaySum.toDouble / taskNum.toDouble
          val avgDuration = durationSum.toDouble / taskNum.toDouble
          val avgSchedulerDelayRatio = avgSchedulerDelay/avgDuration
          stageData.tasks.get.map(_._2.taskLocality).groupBy(locally).foreach(taskLocalityInfo=> {
            val taskNum = taskLocalityInfo._2.size
            seq = seq :+ (taskLocalityInfo._1, taskNum)
          })

          StageParallelSummaryList = StageParallelSummaryList :+ new StageParallelSummary(stageData.stageId,stageData.name, ratio, avgSchedulerDelayRatio , avgSchedulerDelay,avgDuration,taskNum, new TaskLocalitySummary(seq.sortBy(item => item._1)))

        }
       })
      StageParallelSummaryList
    }
  }
}
