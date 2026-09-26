// isolation_forest_runtime.h
// ----------------------------
// Runtime scorer for the model exported by train_isolation_forest.py.
// Include isolation_forest_model.h (the generated file) BEFORE this header.
//
// Usage from the S3 firmware, e.g. once per day at rollover:
//
//   #include "isolation_forest_model.h"   // generated -- copy onto the S3
//   #include "isolation_forest_runtime.h"
//   ...
//   float features[IF_NUM_FEATURES] = {
//     bathroomVisitCountToday, bathroomTotalDurationToday,
//     bathroomMaxDurationToday, wakeTimeSecOfDayToday
//   }; // MUST match the exact order printed in isolation_forest_model.h
//   float score = ifComputeAnomalyScore(features);
//   if (score >= IF_ANOMALY_THRESHOLD) {
//     publishAlert("House", "multivariate_anomaly", score, IF_ANOMALY_THRESHOLD);
//   }
//
// This is an ADDITIONAL check alongside the existing per-metric EWMA
// checks -- it catches combinations of mildly-off features that no single
// threshold would flag on its own. Keep both running.

#pragma once
#include <math.h>

// Average path length of an unsuccessful BST search over n points --
// identical formula to the Python training script's c_factor().
static inline float ifCFactor(float n) {
  if (n <= 1.0f) return 0.0f;
  if (n == 2.0f) return 1.0f;
  const float EULER_MASCHERONI = 0.5772156649f;
  return 2.0f * (logf(n - 1.0f) + EULER_MASCHERONI) - (2.0f * (n - 1.0f) / n);
}

// Walks one tree (identified by its node offset in the flattened arrays)
// for a single feature vector, returning depth-reached + c(leaf samples).
static float ifPathLength(int treeIdx, const float* features) 
{
  int node = if_tree_offsets[treeIdx];
  int depth = 0;
  while (true)
   {
    int left = if_node_left[node];
    int right = if_node_right[node];
    if (left == -1 && right == -1) 
    {
      // Leaf
      return depth + ifCFactor((float)if_node_samples[node]);
    }
    int feat = if_node_feature[node];
    float thresh = if_node_threshold[node];
    node = (features[feat] <= thresh) ? left : right;
    depth++;
  }
}

// Returns the anomaly score in [0,1]-ish range: values near 1.0 indicate
// anomalous, values at/below ~0.5 indicate normal, matching the original
// Isolation Forest paper's convention (Liu, Ting & Zhou, 2008).
float ifComputeAnomalyScore(const float* features) 
{
  float totalH = 0.0f;
  for (int t = 0; t < IF_NUM_TREES; t++) 
  {
    totalH += ifPathLength(t, features);
  }
  float avgH = totalH / (float)IF_NUM_TREES;
  float cn = ifCFactor((float)IF_MAX_SAMPLES);
  return powf(2.0f, -avgH / cn);
}
