/**
 * Copyright 2014 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.plunger;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.local.LocalFlowStep;
import cascading.management.state.ClientState;
import cascading.stats.CascadingStats;
import cascading.stats.local.LocalStepStats;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Writes a {@link Data} instance to a {@link Tap}. Currently works only with hadoop and local taps.
 * <p/>
 * <strong>WARNING:</strong> This is exceedingly brittle as it relies on cascading internals.
 */
public class TapDataWriter {

  private final Data data;

  TapDataWriter(Data data) {
    this.data = data;
  }

  /** Writes the {@link Tuple Tuples} provided in the {@link Data} instance to the supplied {@link Tap}. */
  public Tap<?, ?, ?> toTap(Tap<?, ?, ?> tap) throws IOException {
    Class<?> tapConfigClass = TapTypeUtil.getTapConfigClass(tap);
    if (JobConf.class.equals(tapConfigClass)) {
      writeToHadoopTap(tap);
    } else if (Properties.class.equals(tapConfigClass)) {
      writeToLocalTap(tap);
    } else {
      throw new IllegalArgumentException("Unsupported tap type: " + tap.getClass());
    }
    return tap;
  }

  private void writeToHadoopTap(Tap<?, ?, ?> tap) throws IOException {
    @SuppressWarnings("unchecked")
    Tap<JobConf, ?, ?> hadoopTap = (Tap<JobConf, ?, ?>) tap;
    JobConf conf = new JobConf();
    // WARNING: This is exceedingly brittle as it relies on cascading internals
    conf.setInt("cascading.flow.step", 1);
    HadoopFlowProcess flowProcess = new HadoopFlowProcess(conf);
    hadoopTap.sinkConfInit(flowProcess, conf);
    TupleEntryCollector collector = hadoopTap.openForWrite(flowProcess);
    for (TupleEntry tuple : data.asTupleEntryList()) {
      collector.add(tuple);
    }
    collector.close();
    hadoopTap.commitResource(conf);
  }

  private void writeToLocalTap(Tap<?, ?, ?> tap) throws IOException {
    @SuppressWarnings("unchecked")
    Tap<Properties, ?, ?> localTap = (Tap<Properties, ?, ?>) tap;
    Properties conf = new Properties();
    LocalFlowProcess flowProcess = new LocalFlowProcess(conf);
    // LocalStepStats instance is required for PartitionTap
    flowProcess.setStepStats(new LocalStepStats(new LocalFlowStep("writeToLocalTap:" + tap.getIdentifier(), 0),
        NullClientState.INSTANCE));
    localTap.sinkConfInit(flowProcess, conf);
    TupleEntryCollector collector = localTap.openForWrite(flowProcess);
    for (TupleEntry tuple : data.asTupleEntryList()) {
      collector.add(tuple);
    }
    collector.close();
    localTap.commitResource(conf);
  }

  private static class NullClientState extends ClientState {
    private static NullClientState INSTANCE = new NullClientState();

    private NullClientState() {
    }

    @Override
    public void recordStats(CascadingStats stats) {
    }

    @Override
    public void recordFlowStep(@SuppressWarnings("rawtypes") FlowStep flowStep) {
    }

    @Override
    public void recordFlow(@SuppressWarnings("rawtypes") Flow flow) {
    }

    @Override
    public void recordCascade(Cascade cascade) {
    }
  }

}
