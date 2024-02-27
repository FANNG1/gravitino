/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark;


import com.datastrato.gravitino.rel.types.Types.IntegerType;
import org.apache.spark.sql.types.DataType;
import org.junit.jupiter.api.Test;

public class TestSparkTypeConverter {

  @Test
  void testConvertSparkTypeToGravition() {
    IntegerType integerType = IntegerType.get();
    DataType sparkType = SparkTypeConverter.convert(integerType);

  }

  @Test
  void testConvertGravitionTypeToSpark() {

  }
}
