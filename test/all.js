var {HBase} = require('hbase');

exports.testBasic = function() {
	//var hbase = new HBase("test");
	//print(hbase.get("row1", "column"));
	//hbase.put("blah", "column", "whatever");
	//var r = hbase.scan("a", "b", "column");
	//print(JSON.stringify(r));
}

exports.testEmpty = function() {
	var hbase = new HBase("test");
	hbase.put("blah2", "column", "");
}

/*
var t = 0, c = 0;
function time(test) {
  return function() {
    var start = new Date().getTime();
    test();
    var time = new Date().getTime() - start;
    t += time;
    c ++;
  }
}


for(var i = 0; i < 10; i ++) {
	time(test)();
}

print("time: " + t + "ms");
print("count: " + c);
print("time/count: " + (t / c) + "ms");
*/

if (require.main == module.id) {
    require('test').run(exports);
}