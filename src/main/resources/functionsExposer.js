var functions = new Array();
for(var i in this) {
	if((typeof this[i]).toString() === "function" && this[i].toString().indexOf("native") == -1) {
		functions[functions.length] = this[i].name
	}
}
