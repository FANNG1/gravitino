package org.apache.gravitino.util;

import java.util.List;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStatisticValueUtils {

  @Test
  void avgReturnsNullForEmptyList() {
    Assertions.assertNull(StatisticValueUtils.avg(List.of()));
  }

  @Test
  void avgCalculatesCorrectAverageForLongValues() {
    List<StatisticValue> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.longValue(20L),
            StatisticValues.longValue(30L));
    StatisticValue result = StatisticValueUtils.avg(values);
    Assertions.assertEquals(20.0, result.value());
  }

  @Test
  void avgCalculatesCorrectAverageForDoubleValues() {
    List<StatisticValue> values =
        List.of(
            StatisticValues.doubleValue(10.5),
            StatisticValues.doubleValue(20.5),
            StatisticValues.doubleValue(30.5));
    StatisticValue result = StatisticValueUtils.avg(values);
    Assertions.assertEquals(20.5, result.value());
  }

  @Test
  void sumReturnsNullForEmptyList() {
    Assertions.assertNull(StatisticValueUtils.sum(List.of()));
  }

  @Test
  void sumCalculatesCorrectSumForLongValues() {
    List<StatisticValue> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.longValue(20L),
            StatisticValues.longValue(30L));
    StatisticValue result = StatisticValueUtils.sum(values);
    Assertions.assertEquals(60L, result.value());
  }

  @Test
  void sumCalculatesCorrectSumForDoubleValues() {
    List<StatisticValue> values =
        List.of(
            StatisticValues.doubleValue(10.5),
            StatisticValues.doubleValue(20.5),
            StatisticValues.doubleValue(30.5));
    StatisticValue result = StatisticValueUtils.sum(values);
    Assertions.assertEquals(61.5, result.value());
  }

  @Test
  void divThrowsExceptionForZeroDivisor() {
    StatisticValue value = StatisticValues.longValue(10L);
    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> StatisticValueUtils.div(value, 0));
    Assertions.assertEquals("Divisor cannot be zero", exception.getMessage());
  }

  @Test
  void divCalculatesCorrectDivisionForLongValues() {
    StatisticValue value = StatisticValues.longValue(10L);
    StatisticValue result = StatisticValueUtils.div(value, 2);
    Assertions.assertEquals(5.0, result.value());
  }

  @Test
  void divCalculatesCorrectDivisionForDoubleValues() {
    StatisticValue value = StatisticValues.doubleValue(10.5);
    StatisticValue result = StatisticValueUtils.div(value, 2);
    Assertions.assertEquals(5.25, result.value());
  }

  @Test
  void avgThrowsExceptionForNonNumberValues() {
    List<StatisticValue> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.doubleValue(20.5),
            StatisticValues.stringValue("invalid"));
    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> StatisticValueUtils.avg(values));
    Assertions.assertEquals("Value must be a number", exception.getMessage());
  }
}
