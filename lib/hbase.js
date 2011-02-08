var hbase = org.apache.hadoop.hbase;
var client = hbase.client;
var HBaseConfiguration = hbase.HBaseConfiguration;
var Bytes = hbase.util.Bytes;
var Get = client.Get;
var Delete = client.Delete;
var HTable = client.HTable;
var HTablePool = client.HTablePool;
var Put = client.Put;
var Result = client.Result;
var ResultScanner = client.ResultScanner;
var Scan = client.Scan;

var pool = new HTablePool();

//TODO refactor to use proper var names for columns and column families

var HBase = exports.HBase = function(name) {

	var table = name;
	
	//TODO support multiput
	//TODO filter results by column
	//TODO support multiple columns
	this.put = function(row, column, value) {
		value = value || '';
		var htable = pool.getTable(table);
		try {
			var p = new Put(Bytes.toBytes(row));
			p.add(Bytes.toBytes(column), Bytes.toBytes(""), Bytes.toBytes(value));	
			htable.put(p);
		} finally {
			pool.putTable(htable);
		}
	}
	
	//TODO filter results by column
	//TODO support multiple columns
	this.get = function(rows, column) {
		var htable = pool.getTable(table);
		try {
			var columnBytes = Bytes.toBytes(column), emptyBytes = Bytes.toBytes("");
			if(Array.isArray(rows)) {
				var gets = [], results = [];
				for(var i = 0, l = rows.length; i < l; i ++) {
					gets.push(new Get(Bytes.toBytes(rows[i])));
				}
				var r = htable.batch(java.util.Arrays.asList(gets));
				for(var i = 0, l = r.length; i < l; i ++) {
					var result = r[i];
					var value = result ? result.getValue(columnBytes, emptyBytes) : null;
					results.push(value ? Bytes.toString(value) : null);
				}
				return results;
			} else {
				var r = htable.get(new Get(Bytes.toBytes(rows)));
				var value = r.getValue(columnBytes, emptyBytes);
				return Bytes.toString(value);
			}
		} finally {
			pool.putTable(htable);
		}
	}
	
	this.del = function(rows) {
		var htable = pool.getTable(table);
		try {
			if(Array.isArray(rows)) {
				var deletes = [];
				for(var i = 0, l = rows.length; i < l; i ++) {
					deletes.push(new Delete(Bytes.toBytes(rows[i])));
				}
				htable.batch(java.util.Arrays.asList(deletes));
			} else {
				htable["delete"](new Delete(Bytes.toBytes(rows)));
			}
		} finally {
			pool.putTable(htable);
		}
	}
	
	//TODO support multiple columns
	//TODO filter results by column
	this.scan = function(start, end, column, limit) {
		var htable = pool.getTable(table);
		try {
			var r = [];
			var s = new Scan(Bytes.toBytes(start), Bytes.toBytes(end));
			if(limit) {
				s.setCaching(limit);
			} else {
				limit = -1;
			}
			var columnBytes = Bytes.toBytes(column), emptyBytes = Bytes.toBytes("");
			//TODO support no column
			s.addColumn(columnBytes, emptyBytes);
			var scanner = htable.getScanner(s);
			for (var rr = scanner.next(); rr && limit != 0; rr = scanner.next(), limit --) {
				r.push([Bytes.toString(rr.getRow()),
							 Bytes.toString(rr.getValue(columnBytes, emptyBytes))]);
			}
			return r;
		} finally {
			pool.putTable(htable);
		}
	}
}
