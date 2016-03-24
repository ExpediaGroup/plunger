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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.TupleScheme;

import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@RunWith(MockitoJUnitRunner.class)
public class TupleSchemeTest {

  @Mock
  private SourceCall<Void, Iterator<Tuple>> mockSourceCall;
  @Mock
  private SinkCall<Void, List<Tuple>> mockSinkCall;
  @Mock
  private Iterator<Tuple> mockTupleIterator;
  @Mock
  private TupleEntry mockTupleEntry;
  @Mock
  private Tuple mockTuple;
  @Mock
  private List<Tuple> mockOutput;

  private final TupleScheme scheme = new TupleScheme(new Fields("A"));

  @Test
  public void readTuple() throws IOException {
    when(mockSourceCall.getInput()).thenReturn(mockTupleIterator);
    when(mockSourceCall.getIncomingEntry()).thenReturn(mockTupleEntry);
    when(mockTupleIterator.hasNext()).thenReturn(true);
    when(mockTupleIterator.next()).thenReturn(mockTuple);
    boolean read = scheme.source(null, mockSourceCall);
    verify(mockTupleEntry).setTuple(mockTuple);
    assertThat(read, is(true));
  }

  @Test
  public void emptyRead() throws IOException {
    when(mockSourceCall.getInput()).thenReturn(mockTupleIterator);
    when(mockTupleIterator.hasNext()).thenReturn(false);
    boolean read = scheme.source(null, mockSourceCall);
    verifyZeroInteractions(mockTupleEntry);
    assertThat(read, is(false));
  }

  @Test
  public void sink() throws IOException {
    when(mockSinkCall.getOutput()).thenReturn(mockOutput);
    when(mockSinkCall.getOutgoingEntry()).thenReturn(mockTupleEntry);
    when(mockTupleEntry.getTupleCopy()).thenReturn(mockTuple);
    scheme.sink(null, mockSinkCall);
    verify(mockOutput).add(mockTuple);
  }

}
