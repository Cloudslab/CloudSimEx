package org.cloudbus.cloudsim.incubator.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.google.common.primitives.Primitives;

/**
 * 
 * This is a utility class for transforming beans and other classes into
 * consistent and well aligned text. Can be used to easily generate readable log
 * or CSV files.
 * 
 * @author nikolay.grozev
 * 
 * @see {@link Textualize}
 * 
 */
public class TextUtil {

    /** Format for double precision numbers. */
    public static final DecimalFormat DEC_FORMAT = new DecimalFormat("####0.00");
    /** Number of positions used when converting doubles to text. */
    public static final int SIZE_OF_DBL_STRINGS = 10;
    /** Number of positions used when converting integers to text. */
    public static final int SIZE_OF_INT_STRINGS = 7;
    /** The new line symbol of this system. */
    public static final String NEW_LINE = System.getProperty("line.separator");

    private static final String STANDARD_GET_REGEX = "get.+";
    private static final String BOOLGET_REGEX = "is.+";
    private static final String DEFAULT_DELIM = ";";
    private static final Map<Class<?>, List<Method>> GET_METHODS = new HashMap<Class<?>, List<Method>>();

    /**
     * Converts the specified object to a single line of text. Convenient to
     * converting an object to a line in a log or a line in a CSV file. For the
     * purpose all get methods of the object are consequently called and the
     * results are appended with appropriate formatting. Users, can control
     * which get methods are being called by using the {@link Textualize}
     * annotation and specifying the properties (the parts of the get methods
     * after "get" or "is") and the order they need.
     * 
     * <br/>
     * 
     * Note that if the class is annotated with {@link Textualize} the order
     * specified in the annotation is used. If not - the order of the methods is
     * defined by the class they appear in (this classe's props first, then its
     * superclass and so on). Properties defined within the same class are
     * sorted alphabetically.
     * 
     * @param obj
     *            - the object to extract text from. Must not be null.
     * @return formated line of text, as described above.
     */
    public static String getTxtLine(final Object obj) {
	return getTxtLine(obj, DEFAULT_DELIM, false);
    }

    /**
     * Converts the specified object to a single line of text. Convenient to
     * converting an object to a line in a log or a line in a CSV file. For the
     * purpose all get methods of the object are consequently called and the
     * results are appended with appropriate formatting. Users, can control
     * which get methods are being called by using the {@link Textualize}
     * annotation and specifying the properties (the parts of the get methods
     * after "get" or "is") and the order they need.
     * 
     * <br/>
     * 
     * The flag includeFieldNames is used to specify if the names of the
     * properties should be included in the result. If it is true, the result
     * will consist of entries like: "propA=valueA"
     * 
     * <br/>
     * 
     * Note that if the class is annotated with {@link Textualize} the order
     * specified in the annotation is used. If not - the order of the methods is
     * defined by the class they appear in (this classe's props first, then its
     * superclass and so on). Properties defined within the same class are
     * sorted alphabetically.
     * 
     * @param obj
     *            - the object to extract text from. Must not be null.
     * @param delimeter
     *            - the delimeter to put between the entries in the line. Must
     *            not be null.
     * @param includeFieldNames
     *            - a flag whether to include the names of the properties in the
     *            line as well.
     * @return formated line of text, as described above.
     */
    public static String getTxtLine(final Object obj, final String delimeter, final boolean includeFieldNames) {
	StringBuffer result = new StringBuffer();
	List<Method> methods = extractGetMethodsForClass(obj.getClass());
	int i = 0;
	for (Method m : methods) {
	    Object methodRes = null;
	    try {
		methodRes = m.invoke(obj);
	    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
		methodRes = "ERR " + e.getMessage();
	    }

	    String propName = getPropName(m);
	    String mTxt = toString(methodRes);
	    if (includeFieldNames) {
		result.append(propName + "=" + mTxt);
	    } else {
		if (propName.length() > mTxt.length()) {
		    mTxt = String.format("%" + propName.length() + "s", mTxt);
		}
		result.append(mTxt);
	    }

	    result.append(i < methods.size() - 1 ? delimeter : "");
	    i++;
	}

	return result.toString();
    }

    /**
     * Converts the specified class to a single line of text. Convenient for
     * generating a header line in a log or a CSV file. For the purpose the
     * names of all properties (the parts of the get methods after "get" or
     * "is") are concatenated with appropriate padding and formatting. Users,
     * can control which properties are used by using the {@link Textualize}
     * annotation and specifying the properties and the order they need.
     * 
     * <br/>
     * 
     * Note that if the class is annotated with {@link Textualize} the order
     * specified in the annotation is used. If not - the order of the methods is
     * defined by the class they appear in (this classe's props first, then its
     * superclass and so on). Properties defined within the same class are
     * sorted alphabetically.
     * 
     * 
     * @param clazz
     *            - the class to use to create the line. Must not be null.
     * @return formated line of text, as described above.
     */
    public static String getCaptionLine(final Class<?> clazz) {
	return getCaptionLine(clazz, DEFAULT_DELIM);
    }

    /**
     * Converts the specified class to a single line of text. Convenient for
     * generating a header line in a log or a CSV file. For the purpose the
     * names of all properties (the parts of the get methods after "get"
     * orString.valueOf(obj) "is") are concatenated with appropriate padding.
     * The specified delimeter is placed between the entries in the line. Users,
     * can control which properties are used by using the {@link Textualize}
     * annotation and specifying the properties and the order they need.
     * 
     * <br/>
     * 
     * Note that if the class is annotated with {@link Textualize} the order
     * specified in the annotation is used. If not - the order of the methods is
     * defined by the class they appear in (this classe's props first, then its
     * superclass and so on). Properties defined within the same class are
     * sorted alphabetically.
     * 
     * 
     * @param clazz
     *            - the class to use to create the line. Must not be null.
     * @param delimeter
     *            - the delimeter to put between the entries in the line. Must
     *            not be null.
     * @return formated line of text, as described above.
     */
    public static String getCaptionLine(final Class<?> clazz, final String delimeter) {
	StringBuffer result = new StringBuffer();
	List<Method> methods = extractGetMethodsForClass(clazz);
	int i = 0;
	for (Method m : methods) {
	    String propEntry = getPropName(m);
	    Class<?> returnType = Primitives.wrap(m.getReturnType());

	    if (Double.class.equals(returnType) || Float.class.equals(returnType)
		    && propEntry.length() < SIZE_OF_DBL_STRINGS) {
		propEntry = String.format("%" + SIZE_OF_DBL_STRINGS + "s", propEntry);
	    } else if (Number.class.isAssignableFrom(returnType) && propEntry.length() < SIZE_OF_INT_STRINGS) {
		propEntry = String.format("%" + SIZE_OF_INT_STRINGS + "s", propEntry);
	    }

	    result.append(propEntry);
	    result.append(i < methods.size() - 1 ? delimeter : "");
	    i++;
	}

	return result.toString();
    }

    private static List<Method> extractGetMethodsForClass(Class<?> clazz1) {
	List<Method> methods = null;
	Class<?> clazz = clazz1;
	if (!GET_METHODS.containsKey(clazz)) {
	    methods = new ArrayList<>();
	    do {
		// Defined in the class methods (not inherited)
		List<Method> clazzMethods = Arrays.asList(clazz.getDeclaredMethods());
		// Sort them by name... since getDeclaredMethods does not
		// guarantee order
		Collections.sort(clazzMethods, MethodsAlphaComparator.METHOD_CMP);

		methods.addAll(clazzMethods);
		clazz = clazz.getSuperclass();
	    } while (clazz != null);

	    Textualize classAnnotation = clazz1.getAnnotation(Textualize.class);

	    // Filter methods that are not getters and are not in the annotation
	    // (if annotation is specified)
	    for (ListIterator<Method> iter = methods.listIterator(); iter.hasNext();) {
		Method m = iter.next();
		if (classAnnotation != null && !isAllowedGetter(m, classAnnotation)) {
		    iter.remove();
		} else if (classAnnotation == null && !isGetter(m)) {
		    iter.remove();
		}
	    }

	    // Sort by the order defined in the annotation
	    if (classAnnotation != null) {
		final List<String> props = Arrays.asList(classAnnotation.properties());
		Collections.sort(methods, new MethodsLitIndexComparator(props));
	    }

	    methods = Collections.unmodifiableList(methods);
	    GET_METHODS.put(clazz, methods);
	}
	return GET_METHODS.get(clazz);
    }

    private static String getPropName(Method getter) {
	return isBoolGetter(getter) ? getter.getName().substring(2) :
		isGetter(getter) ? getter.getName().substring(3) : getter.getName();
    }

    private static boolean isAllowedGetter(final Method m, final Textualize annotation) {
	HashSet<String> allowedProperties = new HashSet<>(Arrays.asList(annotation.properties()));
	return isGetter(m) && allowedProperties.contains(getPropName(m));
    }

    private static boolean isGetter(final Method m) {
	return isBoolGetter(m) || isStandardGetter(m);
    }

    private static boolean isStandardGetter(final Method m) {
	return m.getParameterTypes().length == 0 && m.getName().matches(STANDARD_GET_REGEX)
		&& !Primitives.wrap(m.getReturnType()).equals(Boolean.class);
    }

    private static boolean isBoolGetter(final Method m) {
	return m.getParameterTypes().length == 0 && m.getName().matches(BOOLGET_REGEX)
		&& Primitives.wrap(m.getReturnType()).equals(Boolean.class);
    }

    /**
     * Returns the textual representation of the specified object.
     * 
     * @param obj
     *            - the object. Must not be null.
     * @return - the textual representation of the specified object.
     */
    public static String toString(final Object obj) {
	String result = "";
	Class<?> clazz = Primitives.wrap(obj.getClass());
	if (clazz.equals(Double.class) || clazz.equals(Float.class)) {
	    result = String.format("%" + SIZE_OF_DBL_STRINGS + "s", TextUtil.DEC_FORMAT.format(obj));
	} else if (clazz.equals(Boolean.class)) {
	    result = String.valueOf(obj);
	} else if (Number.class.isAssignableFrom(clazz)) {
	    result = String.format("%" + SIZE_OF_INT_STRINGS + "s", obj);
	} else if (obj instanceof Date) {
	    result = getDataFormat().format(obj);
	} else if (obj instanceof Collection<?> || obj.getClass().isArray()) {
	    result = "[...]";
	} else if (obj instanceof Class) {
	    result = ((Class<?>) obj).getSimpleName();
	    // If toString is not predefined ...
	} else if (String.valueOf(obj).contains(String.valueOf(obj.hashCode()))) {
	    result = "ref<" + obj.hashCode() + ">";
	} else {
	    result = String.valueOf(obj);
	}
	return result;
    }

    /**
     * Returns the format for dates.
     * 
     * @return the format for dates.
     */
    public static DateFormat getDataFormat() {
	return new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    }

    private static class MethodsAlphaComparator implements Comparator<Method> {
	static MethodsAlphaComparator METHOD_CMP = new MethodsAlphaComparator();

	private MethodsAlphaComparator() {
	};

	@Override
	public int compare(Method o1, Method o2) {
	    String prop1 = getPropName(o1);
	    String prop2 = getPropName(o2);
	    return prop1.compareTo(prop2);
	}
    }

    private static class MethodsLitIndexComparator implements Comparator<Method> {
	private List<String> properties = null;

	public MethodsLitIndexComparator(List<String> properties) {
	    super();
	    this.properties = properties;
	}

	@Override
	public int compare(Method o1, Method o2) {
	    String prop1 = getPropName(o1);
	    String prop2 = getPropName(o2);
	    return Integer.valueOf(properties.indexOf(prop1)).compareTo(properties.indexOf(prop2));
	}
    }

}
