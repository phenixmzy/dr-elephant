package org.apache.spark.ui

import org.apache.spark.ui.exec.ExecutorTaskSummary

import scala.collection.mutable.LinkedHashMap

class ExecutorTaskSummaryWrapper(executorToTaskSummary:LinkedHashMap[String, ExecutorTaskSummary]) {

  def shuffleWrite(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).shuffleWrite
  def shuffleRead(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).shuffleRead
  def inputBytes(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).inputBytes
  def outputBytes(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).outputBytes

  def duration(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).duration
  def inputRecords(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).inputRecords
  def tasksActive(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).tasksActive
  def tasksComplete(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).tasksComplete
  def tasksFailed(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).tasksFailed

  def totalCores(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).totalCores
  def jvmGCTime(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).jvmGCTime
  def tasksMax(eid:String) = executorToTaskSummary.getOrElse(eid, ExecutorTaskSummary(eid)).tasksMax

}
