package com.nosqldriver.util;

public class JavaScriptEngineWrapper extends ScriptEngineWrapper {
    public JavaScriptEngineWrapper() {
        super("JavaScript");
    }

    // TODO: this replacement is pretty naive. It might corrupt strings that contain equal sign and words "and" and "or"
    public String fixWhereExpression(String expr) {
        return expr.replaceAll("(?<![<>])=", "==")
                .replaceAll("(?i)(\\w+)\\s+between\\s+(\\d+)\\s+and\\s+(\\d+)", "$1>=$2 and $1<=$3")
                .replaceAll("(?i) AND ", " && ").replaceAll("(?i) OR ", " || ").replace("<>", "!=")
                .replaceAll("(?i) like\\s+'%(.*?)%'", ".match(/.*$1.*/)!=null")
                .replaceAll("(?i) like\\s+'%(.*?)'", ".match(/.*$1__ENDOFLINEINLIKEEXPRESSION__/)!=null").replace("__ENDOFLINEINLIKEEXPRESSION__", "$")
                .replaceAll("(?i) like\\s+'(.*?)%'", ".match(/^$1.*/)!=null")
                .replaceAll("(?i)like ", "==");
    }


    @Override
    protected Object unwrapResult(Object obj) {
        if (obj instanceof Double) {
            Double v = (Double)obj;
            Object result = v;
            if (v == Math.floor(v) && !Double.isInfinite(v)) {
                if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
                    result = v.intValue();
                } else {
                    result = v.longValue();
                }
            }
            return result;
        }
        return super.unwrapResult(obj);
    }
}
