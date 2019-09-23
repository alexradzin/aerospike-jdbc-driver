function len(s) {
    return s.length;
}

function ascii(s) {
    return s.length > 0 ? s.charCodeAt(0) : null;
}

function char(code) {
    return String.fromCharCode(code);
}

function charIndex(subStr, str, start) {
    return str.indexOf(subStr, start);
}

function left(str, n) {
    return str.substring(0, n);
}

function lower(str) {
    return str.toLowerCase();
}

function upper(str) {
    return str.toUpperCase();
}

function str(n) {
    return "" + n;
}

function substring(str, start, length) {
    return str.substring(start, length);
}

function space(n) {
    var s = "";
    for (var i = 0; i < n; i++) {
        s += " ";
    }
    return s;
}

function concat() {
    return Array.prototype.slice.call(arguments).join("");
}

function concat_ws(separator) {
    var allArgs = Array.prototype.slice.call(arguments);
    var separator = allArgs[0];
    var elements = allArgs.slice(1);
    return elements.join(separator)
}

function reverse(str) {
    var res = "";
    for (var i = str.length - 1; i >= 0; i--) {
        res += str.charAt(i);
    }
    return res;
}

///////////////////////////////////////////////////////////
// Date and Time functions
///////////////////////////////////////////////////////////
function _parse(str, fmt) {
    var d = fmt ? new java.text.SimpleDateFormat(fmt).parse(str) : com.nosqldriver.util.DateParser.date(str);
    return new Date(d.getTime());
}

/**
 * @arg - (optional) epoch or string representation of date
 */
function date(arg) {
    result = (arg == null || typeof arg === 'undefined') ? new Date() : typeof arg === 'number' ? new Date(arg) : typeof arg === 'string' ? _parse(arg) : typeof arg.getMonth === 'function' ? arg : null;
    if (!result) {
        throw new java.sql.SQLException("Wrong argument " + arg);
    }
    return result;
}

function now() {
    return new Date().getTime();
}

function year(d) {
    return date(d).getYear() + 1900;
}

function month(d) {
    return date(d).getMonth() + 1;
}

function dayofmonth(d) {
    return date(d).getDate();
}

function hour(d) {
    return date(d).getHours();
}

function minute(d) {
    return date(d).getMinutes();
}

function second(d) {
    return date(d).getSeconds();
}

function millisecond(d) {
    return date(d).getMilliseconds();
}

function epoch(str, fmt) {
    return _parse(str, fmt).getTime();
}

function millis(date) {
    return date.getTime();
}
