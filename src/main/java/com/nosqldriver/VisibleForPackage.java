package com.nosqldriver;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * This annotation is intended to mark fields and method that have default access and
 * are really intended for access inside current package only.
 * The annotation is used for definition of exceptional rule of code style checker
 * that otherwise complains on each member that has default access.
 */
@Documented
@Target({TYPE, CONSTRUCTOR, METHOD, FIELD})
public @interface VisibleForPackage {
}
