package com.hotels.plunger;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import cascading.tap.CompositeTap;
import cascading.tap.Tap;

/** Determines {@link Tap} platform category (Hadoop, local, etc.). */
class TapTypeUtil {

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

}
