function len(s) {
    return s.length;
}

function length(s) {
    return s.length
}

function ascii(s) {
    return s.length > 0 ? s.charCodeAt(0) : null;
}

function char(code) {
    return String.fromCharCode(code);
}

function locate(subStr, str, start) {
    var offset = start ? start - 1 : 0;
    return str.indexOf(subStr) + 1 - offset;
}

function instr(str, subStr) {
    return str.indexOf(subStr) + 1;
}

function trim(s) {
    return s.trim();
}

function ltrim(s) {
    return s.replace(/^ */g, '')
}

function rtrim(s) {
    return s.replace(/ *$/g, '')
}

function strcmp(s1, s2) {
    return s1 ? s1.localeCompare(s2) : s2 ? -1 : 0;
}

function left(str, n) {
    return str.substring(0, n);
}

function lower(str) {
    return str.toLowerCase();
}

function lcase(str) {
    return str.toLowerCase();
}

function upper(str) {
    return str.toUpperCase();
}

function ucase(str) {
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

function to_base64(b) {
    return java.util.Base64.getEncoder().encodeToString(b.getBytes());
}

function from_base64(s) {
    return java.util.Base64.getDecoder().decode(s)
}

///////////////////////////////////////////////////////////
// Date and Time functions
///////////////////////////////////////////////////////////
function _parse(str, fmt) {
    var d = fmt ? new java.text.SimpleDateFormat(fmt).parse(str) : com.nosqldriver.util.DateParser.date(str);
    return new java.util.Date(d.getTime());
}

/**
 * @arg - (optional) epoch or string representation of date
 */
function date(arg) {
    if(typeof arg === 'object' ) {
        return arg; // it is java.util.Date
    }
    result = (arg == null || typeof arg === 'undefined') ? new java.util.Date() : typeof arg === 'number' ? new java.util.Date(arg) : typeof arg === 'string' ? _parse(arg) : typeof arg.getMonth === 'function' ? arg : null;
    if (!result) {
        throw new java.sql.SQLException("Wrong argument " + arg + " type: " + (typeof arg));
    }
    return result;
}

function calendar(arg) {
    var d = date(arg);
    var c = java.util.Calendar.getInstance();
    c.setTime(d);
    return c;
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
    var millis = date(d).getTime();
    var ms = millis - Math.floor(millis / 1000) * 1000;
    return ms;
}

function epoch(str, fmt) {
    return _parse(str, fmt).getTime();
}

function millis(date) {
    return date.getTime();
}

function map(str) {
    return JSON.parse(str);
}

function list(str) {
    return com.nosqldriver.util.DataUtil.toList(map(str));
}

function array(str) {
    return com.nosqldriver.util.DataUtil.toArray(map(str));
}