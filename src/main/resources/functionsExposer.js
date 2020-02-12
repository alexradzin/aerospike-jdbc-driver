var functions = new java.util.ArrayList();
for(var i in this) {
	if((typeof this[i]).toString() === "function" && this[i].toString().indexOf("native") === -1) {
		functions.add(this[i].name);
	}
}
