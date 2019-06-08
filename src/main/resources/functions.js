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

function concat() {
    return arguments.join("");
}

function left(str, n) {
    return str.substring(0, n);
}

function lower(str, n) {
    return str.toLowerCase();
}

function upper(str, n) {
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

function reverse(str) {
    var res = "";
    for (var i = str.length - 1; i >= 0; i--) {
        res += str.charAt(i);
    }
    return res;
}


function now() {
    return new Date().getTime();
}

function year() {
    return new Date().getYear() + 1900;
}