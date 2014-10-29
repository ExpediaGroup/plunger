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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.Data;
import com.hotels.plunger.PipeFactory;
import com.hotels.plunger.PlungerFlow;
import com.hotels.plunger.TupleListTap;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@RunWith(MockitoJUnitRunner.class)
public class PipeFactoryTest {

  @Mock
  private PlungerFlow flow;
  @Mock
  private FlowDef flowDef;
  @Captor
  private ArgumentCaptor<TupleListTap> tapCaptor;
  @Captor
  private ArgumentCaptor<Pipe> pipeCaptor;

  private final Fields fields = new Fields("A", String.class);
  private final Data data = new Data(fields, Arrays.asList(new Tuple("value")));

  @Test
  public void typical() {
    when(flow.getFlowDef()).thenReturn(flowDef);

    PipeFactory pipeFactory = new PipeFactory(data, "name", flow);
    pipeFactory.newInstance();

    verify(flowDef).addSource(pipeCaptor.capture(), tapCaptor.capture());

    Pipe capturedPipe = pipeCaptor.getValue();
    assertThat(capturedPipe.getName(), is("name"));

    TupleListTap capturedTap = tapCaptor.getValue();
    assertThat(capturedTap.getSourceFields(), is(fields));

    Iterator<Tuple> input = capturedTap.getInput();
    List<Tuple> tuples = new ArrayList<Tuple>();
    while (input.hasNext()) {
      tuples.add(input.next());
    }
    assertThat(tuples.size(), is(1));
    assertThat(tuples.get(0), is(new Tuple("value")));
  }

}
