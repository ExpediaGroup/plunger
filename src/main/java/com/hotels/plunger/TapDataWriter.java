/**
 * Copyright (C) 2014-2016 Expedia Inc.
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
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.management.state.ClientState;
import cascading.pipe.Group;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStepStats;
import cascading.stats.local.LocalStepStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.partition.BasePartitionTap;
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
  private Configuration conf;

  TapDataWriter(Data data) {
    this.data = data;
  }

  /**
   * Set the {@link Configuration} to use for this {@link Tap}.
   *
   * @param conf {@link Tap} {@link Configuration}
   * @return this object.
   */
  TapDataWriter conf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  /** Writes the {@link Tuple Tuples} provided in the {@link Data} instance to the supplied {@link Tap}. */
  public Tap<?, ?, ?> toTap(Tap<?, ?, ?> tap) throws IOException {
    Class<?> tapConfigClass = TapUtil.getTapConfigClass(tap);
    if (Configuration.class.equals(tapConfigClass)) {
      if (tap instanceof BasePartitionTap) {
        writeToHadoopPartitionTap(tap);
      } else {
        writeToHadoopTap(tap);
      }
    } else if (Properties.class.equals(tapConfigClass)) {
      writeToLocalTap(tap);
    } else {
      throw new IllegalArgumentException("Unsupported tap type: " + tap.getClass());
    }
    return tap;
  }

  /* WARNING: This is exceedingly brittle as it relies on cascading internals */
  private void writeToHadoopTap(Tap<?, ?, ?> tap) throws IOException {
    @SuppressWarnings("unchecked")
    Tap<JobConf, ?, ?> hadoopTap = (Tap<JobConf, ?, ?>) tap;
    JobConf conf = TapUtil.newJobConf(this.conf);

    HadoopFlowProcess flowProcess = new HadoopFlowProcess(conf);
    hadoopTap.sinkConfInit(flowProcess, conf);
    TupleEntryCollector collector = hadoopTap.openForWrite(flowProcess);
    for (TupleEntry tuple : data.asTupleEntryList()) {
      collector.add(tuple);
    }
    collector.close();
  }

  /* WARNING: This is exceedingly brittle as it relies on cascading internals */
  private void writeToHadoopPartitionTap(Tap<?, ?, ?> tap) throws IOException {
    @SuppressWarnings("unchecked")
    BasePartitionTap<JobConf, ?, ?> hadoopTap = (BasePartitionTap<JobConf, ?, ?>) tap;
    JobConf conf = TapUtil.newJobConf(this.conf);

    // Avoids deletion of results when using a partition tap (close() will delete the _temporary before the copy has
    // been done if not in a flow)
    HadoopUtil.setIsInflow(conf);

    HadoopFlowProcess flowProcess = new HadoopFlowProcess(conf);
    hadoopTap.sinkConfInit(flowProcess, conf);
    TupleEntryCollector collector = hadoopTap.openForWrite(flowProcess);
    for (TupleEntry tuple : data.asTupleEntryList()) {
      collector.add(tuple);
    }
    collector.close();

    // We need to clean up the '_temporary' folder
    BasePartitionTap<JobConf, ?, ?> partitionTap = hadoopTap;
    @SuppressWarnings("unchecked")
    String basePath = partitionTap.getParent().getFullIdentifier(flowProcess);
    deleteTemporaryPath(new Path(basePath), FileSystem.get(conf));
  }

  private void deleteTemporaryPath(Path outputPath, FileSystem fileSystem) throws IOException {
    if (fileSystem.exists(outputPath)) {
      Path tmpDir = new Path(outputPath, Hadoop18TapUtil.TEMPORARY_PATH);
      if (fileSystem.exists(tmpDir)) {
        fileSystem.delete(tmpDir, true);
      }
    }
  }

  /* WARNING: This is exceedingly brittle as it relies on cascading internals */
  private void writeToLocalTap(Tap<?, ?, ?> tap) throws IOException {
    @SuppressWarnings("unchecked")
    Tap<Properties, ?, ?> localTap = (Tap<Properties, ?, ?>) tap;
    Properties conf = TapUtil.newJobProperties(this.conf);
    LocalFlowProcess flowProcess = new LocalFlowProcess(conf);

    flowProcess.setStepStats(new LocalStepStats(new NullFlowStep(), NullClientState.INSTANCE));

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
    public void recordStats(@SuppressWarnings("rawtypes") CascadingStats stats) {
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

    @Override
    public void recordFlowNode(FlowNode flowNode) {
    }
  }

  @SuppressWarnings("rawtypes")
  private static class NullFlowStep implements FlowStep<Properties> {

    @Override
    public Collection<Group> getGroups() {
      return null;
    }

    @Override
    public Set<Tap> getSourceTaps() {
      return null;
    }

    @Override
    public Set<Tap> getSinkTaps() {
      return null;
    }

    @Override
    public Set<FlowElement> getSinkElements() {
      return null;
    }

    @Override
    public Set<FlowElement> getSourceElements() {
      return null;
    }

    @Override
    public Map<String, Tap> getTrapMap() {
      return null;
    }

    @Override
    public ElementGraph getElementGraph() {
      return null;
    }

    @Override
    public String getID() {
      return null;
    }

    @Override
    public int getOrdinal() {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Flow<Properties> getFlow() {
      return null;
    }

    @Override
    public String getFlowID() {
      return null;
    }

    @Override
    public String getFlowName() {
      return null;
    }

    @Override
    public Properties getConfig() {
      return null;
    }

    @Override
    public String getStepDisplayName() {
      return null;
    }

    @Override
    public int getSubmitPriority() {
      return 0;
    }

    @Override
    public void setSubmitPriority(int submitPriority) {

    }

    @Override
    public FlowNodeGraph getFlowNodeGraph() {
      return null;
    }

    @Override
    public int getNumFlowNodes() {
      return 0;
    }

    @Override
    public Group getGroup() {
      return null;
    }

    @Override
    public Tap getSink() {
      return null;
    }

    @Override
    public Set<String> getSourceName(Tap source) {
      return null;
    }

    @Override
    public Set<String> getSinkName(Tap sink) {
      return null;
    }

    @Override
    public Tap getSourceWith(String identifier) {
      return null;
    }

    @Override
    public Tap getSinkWith(String identifier) {
      return null;
    }

    @Override
    public Set<Tap> getTraps() {
      return null;
    }

    @Override
    public Tap getTrap(String name) {
      return null;
    }

    @Override
    public boolean containsPipeNamed(String pipeName) {
      return false;
    }

    @Override
    public FlowStepStats getFlowStepStats() {
      return null;
    }

    @Override
    public boolean hasListeners() {
      return false;
    }

    @Override
    public void addListener(FlowStepListener flowStepListener) {

    }

    @Override
    public boolean removeListener(FlowStepListener flowStepListener) {
      return false;
    }

    @Override
    public Map<Object, Object> getConfigAsProperties() {
      return null;
    }

    @Override
    public Map<String, String> getProcessAnnotations() {
      return null;
    }

    @Override
    public void addProcessAnnotation(Enum annotation) {
    }

    @Override
    public void addProcessAnnotation(String key, String value) {
    }

    @Override
    public void setFlowStepStats(FlowStepStats flowStepStats) {
    }

  }

}
