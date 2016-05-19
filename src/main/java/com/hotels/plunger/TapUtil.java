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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;

/** Utility methods for {@linkplain Tap taps} */
class TapUtil {

  /** Determines the type of the configuration type argument of the supplied {@link Tap}. */
  static Class<?> getTapConfigClass(Tap<?, ?, ?> tap) {
    Class<?> currentClass = tap.getClass();
    if (CompositeTap.class.isAssignableFrom(currentClass)) {
      currentClass = ((CompositeTap<?>) tap).getChildTaps().next().getClass();
    }
    while (currentClass != null) {
      if (Tap.class.isAssignableFrom(currentClass)) {
        Type genericSuperclass = currentClass.getGenericSuperclass();
        if (genericSuperclass instanceof ParameterizedType) {
          ParameterizedType tapType = (ParameterizedType) genericSuperclass;
          Type[] typeParameters = tapType.getActualTypeArguments();
          Type configTypeParameter = typeParameters[0];
          if (configTypeParameter instanceof Class) {
            Class<?> configClassParameter = (Class<?>) configTypeParameter;
            return configClassParameter;
          }
        }
      }
      currentClass = currentClass.getSuperclass();
    }
    return null;
  }

  /** Create a new {@link JobConf} which has all the values in the given {@link Configuration} */
  static JobConf newJobConf(Configuration conf) {
    JobConf jobConf = conf == null ? new JobConf() : new JobConf(conf);
    return jobConf;
  }

  /** Create a new {@link Properties} object which has all the values in the given {@link Configuration} */
  static Properties newJobProperties(Configuration conf) {
    Properties jobProperties = new Properties();
    if (conf != null) {
      Map<Object, Object> props = HadoopUtil.createProperties(conf);
      for (Entry<Object, Object> e : props.entrySet()) {
        jobProperties.put(e.getKey(), e.getValue());
      }
    }
    return jobProperties;
  }

}
