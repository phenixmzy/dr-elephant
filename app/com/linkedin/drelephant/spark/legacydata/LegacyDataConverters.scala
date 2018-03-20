/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.legacydata

import java.util.Date

import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.log4j.Logger
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.StageStatus

import scala.collection.JavaConverters
import scala.util.Try

/**
  * Converters for legacy SparkApplicationData to current SparkApplicationData.
  *
  * The converters make a best effort, providing default values for attributes the legacy data doesn't provide.
  * In practice, the Dr. Elephant Spark heuristics end up using a relatively small subset of the converted data.
  */


object LegacyDataConverters {
  import JavaConverters._

  def convert(legacyData: SparkApplicationData): com.linkedin.drelephant.spark.data.SparkApplicationData = {
    com.linkedin.drelephant.spark.data.SparkApplicationData(
      legacyData.getAppId,
      extractAppConfigurationProperties(legacyData),
      extractApplicationInfo(legacyData),
      extractJobDatas(legacyData),
      extractStageDatas(legacyData),
      extractExecutorSummaries(legacyData)
    )
  }

  def extractAppConfigurationProperties(legacyData: SparkApplicationData): Map[String, String] =
    legacyData.getEnvironmentData.getSparkProperties.asScala.toMap

  def extractApplicationInfo(legacyData: SparkApplicationData): ApplicationInfoImpl = {
    val generalData = legacyData.getGeneralData
    new ApplicationInfoImpl(
      generalData.getApplicationId,
      generalData.getApplicationName,
      Seq(
        new ApplicationAttemptInfoImpl(
          Some("1"),
          new Date(generalData.getStartTime),
          new Date(generalData.getEndTime),
          generalData.getSparkUser,
          completed = true
        )
      )
    )
  }

  def extractJobDatas(legacyData: SparkApplicationData): Seq[JobDataImpl] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractJobData(jobId: Int): JobDataImpl = {
      val jobInfo = jobProgressData.getJobInfo(jobId)
      new JobDataImpl(
        jobInfo.jobId,
        jobInfo.jobId.toString,
        description = None,
        submissionTime = None,
        completionTime = None,
        jobInfo.stageIds.asScala.map { _.toInt },
        Option(jobInfo.jobGroup),
        extractJobExecutionStatus(jobId),
        jobInfo.numTasks,
        jobInfo.numActiveTasks,
        jobInfo.numCompletedTasks,
        jobInfo.numSkippedTasks,
        jobInfo.numFailedTasks,
        jobInfo.numActiveStages,
        jobInfo.completedStageIndices.size(),
        jobInfo.numSkippedStages,
        jobInfo.numFailedStages
      )
    }

    def extractJobExecutionStatus(jobId: Int): JobExecutionStatus = {
      if (jobProgressData.getCompletedJobs.contains(jobId)) {
        JobExecutionStatus.SUCCEEDED
      } else if (jobProgressData.getFailedJobs.contains(jobId)) {
        JobExecutionStatus.FAILED
      } else {
        JobExecutionStatus.UNKNOWN
      }
    }

    val sortedJobIds = jobProgressData.getJobIds.asScala.toSeq.sorted
    sortedJobIds.map { jobId => extractJobData(jobId) }
  }

  def extractStageDatas(legacyData: SparkApplicationData): Seq[StageData] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractStageData(stageAttemptId: SparkJobProgressData.StageAttemptId): StageDataImpl = {
      val stageInfo = jobProgressData.getStageInfo(stageAttemptId.stageId, stageAttemptId.attemptId)
      var stageTasks = scala.collection.mutable.Map[Long, TaskDataImpl]()

      val taskDatas = JavaConverters.mapAsScalaMapConverter(stageInfo.tasks).asScala.toMap
      taskDatas.foreach(item => {
        val id = item._1
        val taskData = item._2
        val taskMet = taskData.taskMetrics

        val inputMetricsImpl =
          if (taskMet.inputMetrics != null) Some(new InputMetricsImpl(taskMet.inputMetrics.bytesRead,taskMet.inputMetrics.recordsRead)) else None

        val shuffleReadImpl = if (taskMet.shuffleReadMetrics != null)
          Some(
            new ShuffleReadMetricsImpl(taskMet.shuffleReadMetrics.remoteBlocksFetched,taskMet.shuffleReadMetrics.localBlocksFetched,
              taskMet.shuffleReadMetrics.fetchWaitTime,taskMet.shuffleReadMetrics.remoteBytesRead,
              taskMet.shuffleReadMetrics.localBytesRead,taskMet.shuffleReadMetrics.totalBlocksFetched,
              taskMet.shuffleReadMetrics.recordsRead)
          ) else None

        val shuffleWriteImpl = if (taskMet.shuffleWriteMetrics != null)
          Some(new ShuffleWriteMetricsImpl(taskMet.shuffleWriteMetrics.bytesWritten,taskMet.shuffleWriteMetrics.writeTime,taskMet.shuffleWriteMetrics.recordsWritten))
        else None

        val taskMetricsImpl = new TaskMetricsImpl(
          taskMet.executorDeserializeTime,
          taskMet.executorRunTime,
          taskMet.schedulerDelay,
          taskMet.resultSize,
          taskMet.jvmGcTime,
          taskMet.resultSerializationTime,
          taskMet.memoryBytesSpilled,
          taskMet.diskBytesSpilled,
          inputMetrics = inputMetricsImpl,
          outputMetrics = None,
          shuffleReadMetrics = shuffleReadImpl,
          shuffleWriteMetrics = shuffleWriteImpl
        )

        val taskDataImpl = new TaskDataImpl(
          taskData.taskId,
          taskData.index,
          taskData.attempt,
          new Date(taskData.launchTime),
          taskData.duration,
          taskData.executorId,
          taskData.host,
          taskData.taskLocality,
          taskData.speculative,
          accumulatorUpdates = Seq(),
          errorMessage = None,
          taskMetrics = Some(taskMetricsImpl)
        )
        stageTasks(id) = taskDataImpl
      })

      new StageDataImpl(
        extractStageStatus(stageAttemptId),
        stageAttemptId.stageId,
        stageAttemptId.attemptId,
        stageInfo.numActiveTasks,
        stageInfo.numCompleteTasks,
        stageInfo.numFailedTasks,
        stageInfo.executorRunTime,
        stageInfo.inputBytes,
        inputRecords = 0,
        stageInfo.outputBytes,
        outputRecords = 0,
        stageInfo.shuffleReadBytes,
        shuffleReadRecords = 0,
        stageInfo.shuffleWriteBytes,
        shuffleWriteRecords = 0,
        stageInfo.memoryBytesSpilled,
        stageInfo.diskBytesSpilled,
        stageInfo.name,
        stageInfo.description,
        schedulingPool = "",
        accumulatorUpdates = Seq.empty,
        tasks = Some(stageTasks),
        executorSummary = None
      )
    }

    def extractStageStatus(stageAttemptId: SparkJobProgressData.StageAttemptId): StageStatus = {
      if (jobProgressData.getCompletedStages.contains(stageAttemptId)) {
        StageStatus.COMPLETE
      } else if (jobProgressData.getFailedStages.contains(stageAttemptId)) {
        StageStatus.FAILED
      } else {
        StageStatus.PENDING
      }
    }

    val sortedStageAttemptIds = jobProgressData.getStageAttemptIds.asScala.toSeq.sortBy { stageAttemptId =>
      (stageAttemptId.stageId, stageAttemptId.attemptId)
    }
    sortedStageAttemptIds.map { stageAttemptId => extractStageData(stageAttemptId) }
  }

  def extractExecutorSummaries(legacyData: SparkApplicationData): Seq[ExecutorSummaryImpl] = {
    val executorData = legacyData.getExecutorData

    def extractExecutorSummary(executorId: String): ExecutorSummaryImpl = {
      val executorInfo = executorData.getExecutorInfo(executorId)
      new ExecutorSummaryImpl(
        executorInfo.execId,
        executorInfo.hostPort,
        executorInfo.rddBlocks,
        executorInfo.memUsed,
        executorInfo.diskUsed,
        executorInfo.activeTasks,
        executorInfo.failedTasks,
        executorInfo.completedTasks,
        executorInfo.totalTasks,
        executorInfo.duration,
        executorInfo.inputBytes,
        executorInfo.shuffleRead,
        executorInfo.shuffleWrite,
        executorInfo.maxMem,
        executorInfo.totalGCTime,
        executorLogs = Map.empty
      )
    }

    val sortedExecutorIds = {
      val executorIds = executorData.getExecutors.asScala.toSeq
      Try(executorIds.sortBy { _.toInt }).getOrElse(executorIds.sorted)
    }
    sortedExecutorIds.map { executorId => extractExecutorSummary(executorId) }
  }
}
