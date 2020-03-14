var functions = new java.util.ArrayList();
for(var i in this) {
	if((typeof this[i]).toString() === "function" && this[i].toString().indexOf("native") === -1) {
	    var name = this[i].name;
	    if (typeof name != 'undefined') {
            functions.add(name);
	    }
	}
}
