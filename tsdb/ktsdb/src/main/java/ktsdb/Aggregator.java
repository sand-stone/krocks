package ktsdb;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface Aggregator {

  /**
   * Add a datapoint value to the aggregation.
   * @param value the value to add
   */
  void addValue(double value);

  /**
   * Returns the aggregated value of the data added to the aggregator since the
   * last call to {@code aggregatedValue}.
   * After retrieving the aggregated value, the {@code Aggregator} is
   * automatically cleared of all aggregated values.
   *
   * @return the aggregated value
   */
  double aggregatedValue();
}
