package com.hotels.plunger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AbstractOperationCallStubTest {
  private static Fields argumentField = new Fields("in", String.class);
  private static Fields declaredField = new Fields("out", String.class);

  private class MockOperation extends AbstractOperationCallStub<Void> {

    MockOperation() {
      super(argumentField, declaredField);
    }
  }

  @Test
  public void collectTupleCopiesCollectedTuples() throws Exception {
    MockOperation operation = new MockOperation();
    Tuple tuple = new Tuple("value1");
    operation.getOutputCollector().add(tuple);
    tuple.setString(0, "value2");
    operation.getOutputCollector().add(tuple);
    List<Tuple> tuples = operation.result().asTupleList();
    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0).getString(0), is("value1"));
    assertThat(tuples.get(1).getString(0), is("value2"));
  }

  @Test
  public void collectTupleEntryCopiesCollectedTupleEntries() throws Exception {
    MockOperation operation = new MockOperation();
    TupleEntry tupleEntry = new TupleEntry(declaredField, new Tuple("value1"));
    operation.getOutputCollector().add(tupleEntry);
    tupleEntry.setString(declaredField, "value2");
    operation.getOutputCollector().add(tupleEntry);
    List<TupleEntry> tupleEntries = operation.result().asTupleEntryList();
    assertThat(tupleEntries.size(), is(2));
    assertThat(tupleEntries.get(0).getString(declaredField), is("value1"));
    assertThat(tupleEntries.get(1).getString(declaredField), is("value2"));
  }
}
