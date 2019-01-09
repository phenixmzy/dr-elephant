package com.linkedin.drelephant.spark.legacydata;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by mazhiyong on 18/4/12.
 */
public class SparkSQLData {
    private final Set<SparkJobProgressData.StageAttemptId> _completedStages = new HashSet<SparkJobProgressData.StageAttemptId>();

    public void addCompletedStages(int stageId, int attemptId) {
        _completedStages.add(new SparkJobProgressData.StageAttemptId(stageId, attemptId));
    }

    public Set<SparkJobProgressData.StageAttemptId> getCompletedStages() {
        return _completedStages;
    }

}
