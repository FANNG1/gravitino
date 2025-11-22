/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.optimizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOptimizerCmd {

  @Test
  void test() throws Exception {
    //    Option ids =
    // Option.builder().longOpt("identifiers").hasArgs().valueSeparator(',').build();
    Option ids =
        Option.builder()
            .longOpt("identifiers")
            .hasArgs()
            .valueSeparator(',')
            .desc("Comma separated identifier list")
            .build();

    Options options = new Options();
    options.addOption(ids);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, new String[] {"--identifiers", "db.id1,db.id2,db.id3"});

    String[] values = cmd.getOptionValues("identifiers");
    Assertions.assertEquals(3, values.length);
    Assertions.assertEquals("db.id1", values[0]);
    Assertions.assertEquals("db.id2", values[1]);
    Assertions.assertEquals("db.id3", values[2]);
  }
}
