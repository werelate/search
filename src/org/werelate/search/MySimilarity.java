package org.werelate.search;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.Similarity;

/**
 * Created by Dallan Quass
 * Date: Jun 10, 2008
 */
public class MySimilarity extends Similarity {
   public MySimilarity() {
      super();
   }

   public float computeNorm(String field, FieldInvertState state) {
      return 1;
   }

  public float queryNorm(float sumOfSquaredWeights) {
     return 1.0f;
//    return (float)(1.0 / Math.sqrt(sumOfSquaredWeights));
  }

   /** count >1 occurrence same as 1 occurrence */
  public float tf(float freq) {
    return freq > 0.0 ? 1.0f : 0.0f;
  }

  /** Implemented as <code>1 / (distance + 1)</code>. */
  public float sloppyFreq(int distance) {
    return 1.0f / (distance + 1);
  }

  /** don't boost rare terms */
  public float idf(int docFreq, int numDocs) {
    return 1.0f;
  }

  /** no need to do this */
  public float coord(int overlap, int maxOverlap) {
    return 1.0f;
  }
}
