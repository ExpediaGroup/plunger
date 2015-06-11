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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.coerce.Coercions;

class FieldTypeValidator {

  private FieldTypeValidator() {

  }

  /**
   * Validates that the passed values can be converted to the types in the passed {@link Fields Fields} but only in
   * cases where the creation of a {@link Tuple Tuple} wouldn't throw an Exception (i.e. we are stricter than Cascading
   * in order to reduce hard to debug type-related errors in tests). For example Cascading will coerce a null or a "4"
   * to the boolean false which Plunger won't allow.
   * 
   * @param fields Fields containing the expected types.
   * @param values Values to be validated against the expected types.
   * @return values or an Object[] with length of fields.size() when values is empty or null.
   * @throws IllegalStateException if the validation fails in any way.
   */
  static Object[] validateValues(Fields fields, Object... values) {
    if (values == null || values.length == 0) {
      values = new Object[fields.size()];
    } else if (values.length != fields.size()) {
      throw new IllegalStateException("Value array length not suitable for fields: " + fields);
    }

    for (int i = 0; i < values.length; i++) {
      Class<?> typeClass = fields.getTypeClass(i);
      if (typeClass != null) {
        Object value = values[i];

        if (typeClass.isPrimitive() && value == null) {
          throw new IllegalStateException("null cannot be converted to " + typeClass);
        }

        if (typeClass.isPrimitive() && value != null) {
          Class<?> nonPrimitiveTypeClass = Coercions.asNonPrimitive(typeClass);
          Class<?> nonPrimitiveValueClass = Coercions.asNonPrimitive(value.getClass());
          if (!nonPrimitiveTypeClass.isAssignableFrom(nonPrimitiveValueClass)) {
            throw new IllegalStateException(value.getClass() + " cannot be converted to " + typeClass);
          }
        }
      }
    }
    return values;
  }

}
