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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * For stubbing {@link AggregatorCall} in {@link Aggregator} implementations. Primarily this class stores the context
 * state, but is also used to deliver group and argument {@link TupleEntry TupleEntries} and collect output from the
 * aggregator.
 * <p/>
 * Make sure that you call {@link #nextAggregateCall()} to advance to the next set of argument (and possibly group)
 * values prior to calling {@link Aggregator#aggregate(cascading.flow.FlowProcess, AggregatorCall)}.
 */
public final class AggregatorCallStub<C> extends AbstractOperationCallStub<C> implements AggregatorCall<C> {

  private final Iterator<Entry<TupleEntry, List<TupleEntry>>> groupsIterator;
  private Iterator<TupleEntry> valuesIterator;
  private TupleEntry currentArguments;
  private TupleEntry currentGroup;

  private AggregatorCallStub(Fields fields, Map<TupleEntry, List<TupleEntry>> map) {
    super(fields);
    groupsIterator = map.entrySet().iterator();
  }

  @Override
  public TupleEntry getGroup() {
    return currentGroup;
  }

  @Override
  public TupleEntry getArguments() {
    return currentArguments;
  }

  /** Advances to the next arguments value within the group */
  public AggregatorCallStub<C> nextAggregateCall() {
    currentArguments = valuesIterator.next();
    return this;
  }

  /** Advances to the next group */
  public AggregatorCallStub<C> nextGroup() {
    Entry<TupleEntry, List<TupleEntry>> next = groupsIterator.next();
    currentGroup = next.getKey();
    valuesIterator = next.getValue().iterator();
    return this;
  }

  /** Processes the groups with the provided {@link Aggregator}. */
  public AggregatorCallStub<C> complete(FlowProcess<?> flowProcess, Aggregator<C> aggregator) {
    while (groupsIterator.hasNext()) {
      aggregator.prepare(flowProcess, this);
      aggregator.start(flowProcess, nextGroup());
      while (valuesIterator.hasNext()) {
        aggregator.aggregate(flowProcess, nextAggregateCall());
      }
      aggregator.complete(flowProcess, this);
    }
    aggregator.flush(flowProcess, this);
    aggregator.cleanup(flowProcess, this);
    return this;
  }

  public static class Builder<C> {
    private final Fields groupFields;
    private final Fields nonGroupFields;
    private TupleEntry currentGroup;
    private List<TupleEntry> currentValues;
    private Fields outputFields;
    private final Map<TupleEntry, List<TupleEntry>> map = new LinkedHashMap<TupleEntry, List<TupleEntry>>();

    Builder(Fields groupFields, Fields nonGroupFields) {
      if (groupFields == null) {
        throw new IllegalArgumentException("groupFields == null");
      }
      if (nonGroupFields == null) {
        throw new IllegalArgumentException("nonGroupFields == null");
      }
      this.groupFields = groupFields;
      this.nonGroupFields = nonGroupFields;
    }

    /** Specify the output fields when they are different to the nonGroupFields. */
    public Builder<C> outputFields(Fields outputFields) {
      if (outputFields == null) {
        throw new IllegalArgumentException("outputFields == null");
      }
      this.outputFields = outputFields;
      return this;
    }

    /** Creates a new group in the stub record sequence. */
    public Builder<C> newGroup(Object... values) {
      if (values == null || values.length == 0) {
        values = new Object[groupFields.size()];
      } else if (values.length != groupFields.size()) {
        throw new IllegalStateException("Value array length not suitable for group field mask: " + groupFields);
      }
      flush();
      currentGroup = new TupleEntry(groupFields, new Tuple(values));
      currentValues = new ArrayList<TupleEntry>();
      return this;
    }

    /** Creates a new tuple for the current group in the stub record sequence. */
    public Builder<C> addTuple(Object... values) {
      if (currentGroup == null) {
        throw new IllegalStateException("Must set group before adding tuples.");
      }
      if (values == null || values.length == 0) {
        values = new Object[nonGroupFields.size()];
      } else if (values.length != nonGroupFields.size()) {
        throw new IllegalStateException("Value array length not suitable for non-group field mask: " + nonGroupFields);
      }
      currentValues.add(new TupleEntry(nonGroupFields, new Tuple(values)));
      return this;
    }

    /** Builds the stub instance. */
    public AggregatorCallStub<C> build() {
      Fields fields = outputFields != null ? outputFields : nonGroupFields;
      flush();
      return new AggregatorCallStub<C>(fields, map);
    }

    private void flush() {
      if (currentGroup != null && currentValues.size() > 0) {
        map.put(currentGroup, currentValues);
      }
    }

  }

}
