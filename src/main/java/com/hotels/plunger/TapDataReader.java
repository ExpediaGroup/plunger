/**
 * Copyright (C) 2014-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.plunger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 * Reads data from {@link Tap Taps} and returns them as a {@link Data} instance. Currently works only with hadoop and
 * local taps.
 */
class TapDataReader {

  private final Tap<?, ?, ?> source;

  TapDataReader(Tap<?, ?, ?> source) {
    this.source = source;
  }

  /**
   * Reads the {@link Tuple Tuples} from the {@link Tap} and returns them wrapped in a {@link Data} instance whose
   * {@link Fields} confirm to those supplied by {@link Tap#getSourceFields()}.
   */
  Data read() throws IOException {
    TupleEntryIterator tuples = null;
    try {
      Class<?> tapConfigClass = TapTypeUtil.getTapConfigClass(source);

      if (Configuration.class.equals(tapConfigClass)) {
        tuples = getHadoopTupleEntryIterator();
      } else if (Properties.class.equals(tapConfigClass)) {
        tuples = getLocalTupleEntryIterator();
      } else {
        throw new IllegalArgumentException("Unsupported tap type: " + source.getClass());
      }
      List<Tuple> resultTuples = new ArrayList<Tuple>();
      Tuple copy = null;
      while (tuples.hasNext()) {
        copy = tuples.next().getTupleCopy();
        resultTuples.add(copy);
        System.err.println("After adding...");
        System.err.println("resultTuples: " + resultTuples);
        System.err.println("copy: " + copy);
      }
      System.err.println("After while loop...");
      System.err.println("resultTuples: " + resultTuples);
      System.err.println("copy: " + copy);
      
      return new Data(source.getSourceFields(), Collections.unmodifiableList(resultTuples));
    } finally {
      if (tuples != null) {
        tuples.close();
      }
    }
  }

  private TupleEntryIterator getHadoopTupleEntryIterator() throws IOException {
    @SuppressWarnings("unchecked")
    Tap<JobConf, ?, ?> hadoopTap = (Tap<JobConf, ?, ?>) source;
    JobConf conf = new JobConf();
    FlowProcess<JobConf> flowProcess = new HadoopFlowProcess(conf);
    hadoopTap.sourceConfInit(flowProcess, conf);
    return hadoopTap.openForRead(flowProcess);
  }

  private TupleEntryIterator getLocalTupleEntryIterator() throws IOException {
    @SuppressWarnings("unchecked")
    Tap<Properties, ?, ?> localTap = (Tap<Properties, ?, ?>) source;
    Properties properties = new Properties();
    FlowProcess<Properties> flowProcess = new LocalFlowProcess(properties);
    localTap.sourceConfInit(flowProcess, properties);
    return localTap.openForRead(flowProcess);
  }

}
