var application_root = __dirname,
	nano =  require('nano')('http://localhost:5984'),
	express = require('express'),
	detail = require('./routes/detail.js'),
	path = require('path');

var app = express();

app.configure(function () {
	app.use(express.json({limit: '50mb'}));
	app.use(express.methodOverride());
	app.use(app.router);
	app.use(express.static(path.join(application_root, "public")));
	app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

detail.bind(app, '/api/gcdetail')

app.listen(8124);
console.log('Listening on port 8124');
