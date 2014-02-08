var application_root = __dirname,
	nano =  require('nano')('http://localhost:5984'),
        express = require('express'),
        path = require('path');

var app = express();

app.configure(function () {
	app.use(express.bodyParser());
	app.use(express.methodOverride());
	app.use(app.router);
	app.use(express.static(path.join(application_root, "public")));
	app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

// to receive json in a post, The request has to be sent with: content-type: "application/json; charset=utf-8"

app.get('/api/gcdetail/:id', function(req, res){
	var id = parseInt(req.params.id);
	var dbname = 'gc_' + Math.floor(id / 25000);
	var db = nano.db.use(dbname);
	db.get('gc_' + id + '_detail', {}, function(err, body) {
		//console.log(err, body);
		var data = body;
		if(err) {
			data = err;
		}
		//res.setHeader('Content-Type', 'text/plain');
		//res.setHeader('Content-Length', Buffer.byteLength(data));
		res.json(data);
		res.end(data);
	});

});


app.listen(8124);
console.log('Listening on port 8124');

