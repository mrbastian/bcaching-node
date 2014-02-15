var nano =  require('nano')('http://localhost:5984'),
	async = require('async'),
	_ = require('underscore')._;

module.exports.bind = function(app,base) {
	base = base || '/detail';
	app.get(base + '/:id', findById);
	app.post(base, saveDetailBatch);
	app.put(base + '/:id', saveDetail);
	app.delete(base + '/:id', deleteDetail);
}

// to receive json in a post, The request has to be sent with: content-type: "application/json; charset=utf-8"
function getDBName(id) {
	return 'gc_' + Math.floor(id / 25000);
}

var _dbnames = {}
function getDB(id, callback) {
	var dbname = getDBName(id);
	if(_dbnames[dbname]) {
		callback(null, nano.use(dbname))
	} else {
		nano.db.get(dbname, function(err, body) {
			if(err) {
				nano.db.create(dbname, function(err, body) {
					callback(err, nano.use(dbname));
				});
			} else {
				callback(null, nano.use(dbname));
			}
		});
	}
}

function getDocsById(id, callback) {
	var basekey = 'gc_' + id;
	getDB(id, function(err, db) {
		db.get('_all_docs', { startkey: basekey, endkey: basekey + '~', include_docs: true }, function(err, body) {
			if(err) {
				callback(err);
			} else {
				var bodydata = { db: db, basekey: basekey };
				if(body.rows && body.rows.length > 0) {
					var basekeylen = basekey.length + 1;
					for(var i=0; i<body.rows.length; i++) {
						var doc = body.rows[i].doc;
						bodydata[doc._id.substring(basekeylen)] = doc;
					}
				}
				callback(null, bodydata);
			}
		});
	});
}

function saveGc(gc, callback) {
	getDocsById(gc._id, function(err, data) {
		if(err) {
			callback(err);
		} else {
			var docs = [];

			// update detail
			if(gc.detail && (!data || !data.detail || !_.isEqual(data.detail.value, gc.detail))) {
				var doc = { 
					_id: data.basekey + '_detail', 
					value: gc.detail
				};
				if(data.detail) {
					doc._rev = data.detail._rev;
				}
				docs.push(doc);
			}
			
			// update tbs
			if(gc.travelbugs && (!data || !data.tbs || !_.isEqual(data.tbs.value, gc.travelbugs))) {
				var doc = {
					_id: data.basekey + '_tbs',
					value: gc.travelbugs
				};
				if(data.tbs) {
					doc._rev = data.tbs._rev;
				}
				docs.push(doc);
			}

			// merge logs
			if(gc.logs) {
				var logs = gc.logs;

				if(data && data.logs) {
					var logsUpdated = false;
					var newLogsLookup = _.indexBy(gc.logs, '_id');
					logs = data.logs.value;
					for(var i=0; i<logs.length; i++) {
						var oldLog = logs[i];
						var newLog = newLogsLookup[oldLog._id]
						if(newLog && !_.isEqual(oldLog, newLog)) {
							logs.splice(i, 1, newLog);
							logsUpdated = true;
						}
						delete newLogsLookup[oldLog._id];
					};
					for(var k in newLogsLookup) {
						logs.push(newLogsLookup[k]);
						logsUpdated = true;
					}
					
					if(logsUpdated) {
						_.sortBy(logs, function(o){ return o.date; });
						logs.reverse();
					} else {
						logs = null;
					}
				}
				
				if(logs) {
					var doc = {
						_id: data.basekey + '_logs',
						value: logs
					};
					if(data.logs) {
						doc._rev = data.logs._rev;
					}
					docs.push(doc)
				}
			}

			var result = { _id: gc._id, status: 'ok', updates: docs.length };
			if(docs.length == 0) {
				callback(null, result);
			} else {
				//console.log('updating docs', docs);
				data.db.bulk({
					docs: docs
				},
				null,
				function(err, response) {
					if(err) {
						callback(err);
					} else {
						callback(null, response);
					}
				});
				return;
			}
		}
	});
}

function getGcById(id, callback) {
	getDocsById(id, function(err, data) {
		if(err) {
			callback(err);
		} else {
			var gc = { detail: data.detail.value };
			if(data.logs) {
				gc.logs = data.logs.value;
			}
			if(data.tbs) {
				gc.travelbugs = data.tbs.value;
			}
			callback(null, gc);
		}
	});
}

// ----  Express service calls:

function findById(req, res) {
	var id = parseInt(req.params.id);
	getGcById(id, function(err, gc) {
		if(err) {
			res.json(err);
		} else {
			res.json(gc);
		}
		res.end();
	});
}

//curl -X PUT -H "Content-Type: application/json" http://localhost:8124/api/gcdetail/195010 -d @gc195010.data
function saveDetail(req, res) {
	saveGc(req.body, function(err, data) {
		if(err) {
			res.json(err);
		} else {
			res.json(data);
		}
		res.end();
	});
}

function saveDetailBatch(req, res) {
	var results = [];
	async.eachSeries(req.body.docs,
		function(item, callback) {
			saveGc(item, function(err, data) {
				if(err) {
					callback(err);
				} else {
					results.push(data);
					callback();
				}
			});
		},
		function(err) {
			if(err) {
				res.json(err);
			} else {
				res.json(results);
			}
			res.end();
		}
	);
}

function deleteDetail(req, res) {
}
