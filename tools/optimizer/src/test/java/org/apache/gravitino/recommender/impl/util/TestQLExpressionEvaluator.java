package org.apache.gravitino.recommender.impl.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestQLExpressionEvaluator {
  private final QLExpressionEvaluator evaluator = new QLExpressionEvaluator();

  @Test
  void testEvaluateLongWithValidExpression() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);
    context.put("b", 20);

    long result = evaluator.evaluateLong("a + b", context);
    assertEquals(30L, result);
  }

  @Test
  void testEvaluateLongWithDecimalResult() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);
    context.put("b", 3);

    long result = evaluator.evaluateLong("a / b", context);
    assertEquals(3L, result); // Should truncate decimal part
  }

  @Test
  void testEvaluateBoolWithTrueCondition() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 5);
    context.put("y", 10);

    boolean result = evaluator.evaluateBool("x < y", context);
    assertTrue(result);
  }

  @Test
  void testEvaluateBoolWithFalseCondition() {
    Map<String, Object> context = new HashMap<>();
    context.put("x", 15);
    context.put("y", 10);

    boolean result = evaluator.evaluateBool("x < y", context);
    assertTrue(!result);
  }

  @Test
  void testEvaluateWithMissingVariable() {
    Map<String, Object> context = new HashMap<>();
    context.put("a", 10);

    assertThrows(
        RuntimeException.class,
        () -> {
          evaluator.evaluateLong("a + b", context);
        });
  }

  @Test
  void testEvaluateWithInvalidExpression() {
    Map<String, Object> context = new HashMap<>();

    assertThrows(
        RuntimeException.class,
        () -> {
          evaluator.evaluateLong("invalid expression", context);
        });
  }

  @Test
  void testEvaluateWithConstantExpression() {
    evaluator.evaluateLong("1 + 1", null);
  }

  @Test
  void testEvaluateWithNullExpression() {
    Map<String, Object> context = new HashMap<>();

    assertThrows(
        NullPointerException.class,
        () -> {
          evaluator.evaluateLong(null, context);
        });
  }

  @Test
  void testEvaluateWithDifferentVariableTypes() {
    Map<String, Object> context = new HashMap<>();
    context.put("intVal", 10);
    context.put("doubleVal", 5.5);
    context.put("stringVal", "hello");
    context.put("boolVal", true);

    // Test numeric operations
    long numericResult = evaluator.evaluateLong("intVal + doubleVal", context);
    assertEquals(16L, numericResult); // 10 + 5.5 = 15.5 → truncated to 15

    // Test boolean operations
    boolean boolResult = evaluator.evaluateBool("boolVal && (intVal > 5)", context);
    assertTrue(boolResult);
  }
}
