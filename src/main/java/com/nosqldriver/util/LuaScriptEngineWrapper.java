package com.nosqldriver.util;

import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class LuaScriptEngineWrapper extends ScriptEngineWrapper {
    private static final Map<Class, Function<LuaValue, Object>> fromLuaValue = new HashMap<>();
    static {
        fromLuaValue.put(LuaValue.NIL.getClass(), null);
        fromLuaValue.put(Byte.class, LuaValue::tobyte);
        fromLuaValue.put(Short.class, LuaValue::toshort);
        fromLuaValue.put(Integer.class, LuaValue::toint);
        fromLuaValue.put(Long.class, LuaValue::tolong);
        fromLuaValue.put(Float.class, LuaValue::tofloat);
        fromLuaValue.put(Double.class, LuaValue::todouble);

        fromLuaValue.put(LuaInteger.class, LuaValue::toint);
        fromLuaValue.put(LuaBoolean.class, LuaValue::toboolean);
        fromLuaValue.put(LuaDouble.class, luaValue -> {
            double v = luaValue.todouble();
            return ((v == Math.floor(v)) && !Double.isInfinite(v)) ? luaValue.tolong() : v;
        });
        fromLuaValue.put(LuaUserdata.class, luaValue -> ((LuaUserdata)luaValue).userdata());
    }

    private final Map<Class, Function<Object, LuaValue>> toLuaValue = new HashMap<>();
    {
        toLuaValue.put(Double.class, value -> LuaValue.valueOf((double)value));
        toLuaValue.put(Float.class, value -> LuaValue.valueOf((float)value));
        toLuaValue.put(Long.class, value -> LuaValue.valueOf((long)value));
        toLuaValue.put(Integer.class, value -> LuaValue.valueOf((int)value));
        toLuaValue.put(Short.class, value -> LuaValue.valueOf((short)value));
        toLuaValue.put(Byte.class, value -> LuaValue.valueOf((byte)value));
        toLuaValue.put(String.class, value -> LuaValue.valueOf((String)value));
        toLuaValue.put(byte[].class, value -> {
            LuaString luaString = LuaString.valueOf((byte[])value);
            Bindings b = getBindings(ScriptContext.ENGINE_SCOPE);
            b.put(bytesWrapperKey(luaString), "bytes");
            return luaString;
        });
    }


    public LuaScriptEngineWrapper() {
        super("luaj");
    }

    protected String wrapScript(String script) {
        return "return " + script;
    }

    protected Object unwrapResult(Object obj) {
        return obj instanceof LuaValue ? fromLuaValue((LuaValue)obj, null) : obj;
    }

    // TODO: fix for Lua!
    public String fixWhereExpression(String expr) {
        return expr.replaceAll("(?<![<>])=", "==")
                .replaceAll("(?i)(\\w+)\\s+between\\s+(\\d+)\\s+and\\s+(\\d+)", "$1>=$2 and $1<=$3")
                .replace("<>", "~=")
                .replace("!=", "~=")
                .replaceAll(" AND ", " and ").replaceAll(" OR ", " or ")
                .replaceAll("(\\w+) (?i)like '%(\\w+)%'", "string.find($1, '$2')~=nil")
                .replaceAll("(\\w+) (?i)like\\s+'%(.*?)'", "string.find($1, '.*$2__ENDOFLINEINLIKEEXPRESSION__')~=nil").replace("__ENDOFLINEINLIKEEXPRESSION__", "$")
                .replaceAll("(\\w+) (?i)like\\s+'(.*?)%'", "string.find($1, '^$2.*')~=nil")
                .replaceAll("(\\w+) (?i)like ", "$1==");
    }



    @Override
    public void put(String key, Object value) {
        super.put(key, wrap(value));
    }

    private Object wrap(Object obj) {
        if (obj instanceof Supplier) {
            return CoerceJavaToLua.coerce(new LuaSupplierWrapper((Supplier)obj));
        }
        if (obj instanceof Function) {
            return CoerceJavaToLua.coerce(new LuaFunctionWrapper((Function)obj));
        }
        if (obj instanceof BiFunction) {
            return CoerceJavaToLua.coerce(new LuaBiFunctionWrapper((BiFunction)obj));
        }
        if (obj instanceof TriFunction) {
            return CoerceJavaToLua.coerce(new LuaTriFunctionWrapper((TriFunction)obj));
        }
        if (obj instanceof VarargsFunction) {
            return CoerceJavaToLua.coerce(new LuaVarargsFunctionWrapper((VarargsFunction)obj));
        }
        return obj;
    }

    private class LuaSupplierWrapper extends ZeroArgFunction {
        private final Supplier function;

        private LuaSupplierWrapper(Supplier function) {
            this.function = function;
        }

        @Override
        public LuaValue call() {
            return toLuaValue(function.get());
        }
    }

    private class LuaFunctionWrapper extends OneArgFunction {
        private final Function function;
        private final Type paramType;

        private LuaFunctionWrapper(Function function) {
            this.function = function;
            paramType = getGenericTypes(function, 1)[0];
        }

        @Override
        @SuppressWarnings("unchecked")
        public LuaValue call(LuaValue arg) {
            return toLuaValue(function.apply(fromLuaValue(arg, paramType)));
        }
    }

    private class LuaBiFunctionWrapper extends TwoArgFunction {
        private final BiFunction function;
        private final Type[] paramTypes;

        private LuaBiFunctionWrapper(BiFunction function) {
            this.function = function;
            paramTypes = getGenericTypes(function, 2);
        }

        @Override
        @SuppressWarnings("unchecked")
        public LuaValue call(LuaValue arg1, LuaValue arg2) {
            return toLuaValue(function.apply(fromLuaValue(arg1, paramTypes[0]), fromLuaValue(arg2, paramTypes[1])));
        }
    }

    private class LuaTriFunctionWrapper extends ThreeArgFunction {
        private final TriFunction function;
        private final Type[] paramTypes;

        private LuaTriFunctionWrapper(TriFunction function) {
            this.function = function;
            paramTypes = getGenericTypes(function, 3);
        }

        @Override
        @SuppressWarnings("unchecked")
        public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
            return toLuaValue(function.apply(fromLuaValue(arg1, paramTypes[0]), fromLuaValue(arg2, paramTypes[1]), fromLuaValue(arg3, paramTypes[2])));
        }
    }

    private class LuaVarargsFunctionWrapper extends VarArgFunction {
        private final VarargsFunction function;

        private LuaVarargsFunctionWrapper(VarargsFunction function) {
            this.function = function;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Varargs invoke(Varargs vargs) {
            int n = vargs.narg();
            Object[] args = new Object[n];
            for (int i = 0; i < n; i++) {
                args[i] = fromLuaValue(vargs.arg(i + 1), null);
            }
            return toLuaValue(function.apply(args));
        }
    }


    private LuaValue toLuaValue(Object value) {
        if (value == null) {
            return LuaValue.NIL;
        }
        return toLuaValue.getOrDefault(value.getClass(), LuaValue::userdataOf).apply(value);

    }

    private String bytesWrapperKey(LuaString luaString)  {
        return "bytes." + System.identityHashCode(luaString);
    }



    public Object fromLuaValue(LuaValue luaValue, Type requiredType) {
        if (LuaValue.NIL.equals(luaValue)) {
            return null;
        }

        if (requiredType != null && (luaValue instanceof LuaDouble || luaValue instanceof LuaInteger)) {
            return fromLuaValue.get(requiredType).apply(luaValue);
        }

        if (luaValue instanceof LuaString) {
            return byte[].class.equals(requiredType) || getBindings(ScriptContext.ENGINE_SCOPE).containsKey(bytesWrapperKey((LuaString)luaValue)) ?
                    ((LuaString)luaValue).m_bytes :
                    luaValue.tojstring();
        }

        return fromLuaValue.get(luaValue.getClass()).apply(luaValue);
    }

    private Type[] getGenericTypes(Object obj, int n) {
        return Arrays.stream(obj.getClass().getGenericInterfaces())
                .filter(i -> i instanceof ParameterizedType)
                .filter(i -> ((Class)((ParameterizedType)i).getRawType()).getAnnotation(FunctionalInterface.class) != null)
                .map(i -> ((ParameterizedType)i).getActualTypeArguments())
                .findFirst()
                .orElse(new Type[n]);
    }

}
