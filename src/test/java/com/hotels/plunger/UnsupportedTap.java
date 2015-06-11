/**
 * Copyright 2015 Expedia Inc.
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

import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.PropertySanitizer;
import cascading.management.annotation.Visibility;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

class UnsupportedTap extends Tap<String, Long, Integer> {

  private static final long serialVersionUID = 1L;

  @Override
  @Property(name = "identifier", visibility = Visibility.PUBLIC)
  @PropertyDescription("The resource this Tap instance represents")
  @PropertySanitizer("cascading.management.annotation.URISanitizer")
  public String getIdentifier() {
    return "";
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<? extends String> flowProcess, Long input) throws IOException {
    return null;
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess<? extends String> flowProcess, Integer output) throws IOException {
    return null;
  }

  @Override
  public boolean createResource(String conf) throws IOException {
    return false;
  }

  @Override
  public boolean deleteResource(String conf) throws IOException {
    return false;
  }

  @Override
  public boolean resourceExists(String conf) throws IOException {
    return false;
  }

  @Override
  public long getModifiedTime(String conf) throws IOException {
    return 0;
  }
}
