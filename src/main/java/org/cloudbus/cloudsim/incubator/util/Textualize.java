package org.cloudbus.cloudsim.incubator.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for types, which specified which properties of the class should
 * for printing/textualizing.
 * 
 * @author nikolay.grozev
 * 
 * @see {@link TextUtil}
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Textualize {

    /**
     * Returns the properties which are textualizable.
     * @return which properties of the annotated type are textualizable in a
     *         single line.
     */
    String[] properties();
}
