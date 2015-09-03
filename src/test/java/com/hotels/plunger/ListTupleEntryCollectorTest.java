/**
 * Copyright (C) 2015 Expedia Inc.
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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.LastModifiedCallback;
import com.hotels.plunger.ListTupleEntryCollector;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@RunWith(MockitoJUnitRunner.class)
public class ListTupleEntryCollectorTest {

  @Mock
  private List<Tuple> mockOutput;
  @Mock
  private LastModifiedCallback mockCallback;
  @Mock
  private TupleEntry mockTupleEntry;

  @Test
  public void typical() throws IOException {
    Tuple capturedTuple = new Tuple();
    when(mockTupleEntry.getTupleCopy()).thenReturn(capturedTuple);
    ListTupleEntryCollector collector = new ListTupleEntryCollector(mockOutput, mockCallback);
    collector.collect(mockTupleEntry);
    verify(mockOutput).add(capturedTuple);
    verify(mockCallback).modified();
  }

}
