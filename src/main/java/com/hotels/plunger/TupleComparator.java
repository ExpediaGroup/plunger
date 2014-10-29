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

import java.util.Comparator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Compares {@link Tuple Tuples} based on the supplied sort fields. It is expected that the sort fields are a subset of
 * the declared fields and that the values in those fields can be cast to {@link Comparable}. Used only by
 * plunger classes, intentionally not part of the public API.
 */
class TupleComparator implements Comparator<Tuple> {

  private final Fields declaredFields;
  private final Fields sortFields;

  TupleComparator(Fields declaredFields, Fields sortFields) {
    if (Fields.merge(declaredFields, sortFields).size() != declaredFields.size()) {
      throw new IllegalArgumentException("Declared fields must contain sort fields: sortFields=" + sortFields
          + ", declaredFields=" + declaredFields);
    }
    this.declaredFields = declaredFields;
    this.sortFields = sortFields;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(Tuple o1, Tuple o2) {
    for (Comparable<?> field : sortFields) {
      int position = declaredFields.getPos(field);
      Comparable value1 = (Comparable) o1.getObject(position);
      Comparable value2 = (Comparable) o2.getObject(position);
      if (value1 == null && value2 != null) {
        return -1;
      }
      if (value1 != null && value2 == null) {
        return 1;
      }
      if (value1 == null && value2 == null) {
        continue;
      }
      int compareTo = value1.compareTo(value2);
      if (compareTo != 0) {
        return compareTo;
      }
    }
    return 0;
  }

}
