"use strict";

var util = require('util')
var zookeeper = require('node-zookeeper-client')
var zkClient
var _ = require('underscore')
var bigstore = {}

module.exports = function(opts) {
	var seneca = this
	var plugin = 'seneca-zookeeper-registry'
	var store = {}

	seneca.add( {role:plugin, cmd:'create'}, cmd_create)
	seneca.add( {role:plugin, cmd:'set'},    cmd_set)
	seneca.add( {role:plugin, cmd:'get'},    cmd_get)
	seneca.add( {role:plugin, cmd:'list'},   cmd_list)
	seneca.add( {role:plugin, cmd:'remove'}, cmd_remove)

	seneca.add( {init:plugin}, function(args, done) {
		zkconnect(opts.server, opts.port, function(error, result) {
			if (error) { return done(error) }

			seneca.log.info( util.format('connected to zookeeper @ %s:%s', opts.server, opts.port) )
			done()
		})
	})

	function cmd_create( args, done ) { 
		createznode(args.key, args.value, done)
	}

	function cmd_set( args, done ) {
		setznodedata(args.key, args.value, args.version, done)
	}

	function cmd_get( args, done ) {
		getznodedata(args.key, done)
	}

	function cmd_list( args, done ) {
		var recurse = args.recurse || false

		store[args.key] =  {}
		listchildren(args.key, args.key, store, recurse, loadchildren, function childrenloaded(error, result) {
			if (error) { 
				done(error) 
			}

			done(null, store)
		})

	}

	function cmd_remove( args, done ) {
		removeznode(args.key, done)
	}

	return {
		name: plugin,
		exportmap: {
			store:  function() { return store}
		} 
	}

}


function zkconnect(server, port, cb) {	
	zkClient = zookeeper.createClient( util.format('%s:%s', server, port) )

	zkClient.once('connected', function() {
		cb()
	})

	zkClient.connect()
}


function znodeexists(path, cb) {
	zkClient.exists(path, function (error, stat) {
	    if (error) {
	        return cb(error)
	    }

	    if (stat) {
	        return cb(null, true)
	    } else {
	        return cb(null, false)
	    }
	});

}

// create if the path does not exist	
function createznode(path, value, cb) {
	znodeexists(path, function existsCB (error, result) {
		if (error) {
			return cb(error)
		}

		if (!result) {
			zkClient.create(
			    path,
			    new Buffer(value, "utf-8"),
				null, 
			    function(error, stat) {
			    	if (error) {
			    		return cb(error)
			    	}

			    	return cb(null, stat)	
			    }
			)
		} else {
			cb(null, util.format('znode exists %s ', path))
		}
	})
}

function setznodedata(path, value, version, cb) {
	zkClient.setData(
		path, 
		new Buffer(value), 
		// version, 
		function (error, stat) {
		    if (error) {
		        return cb(error)
		    }

		    cb()
		}
	)
}

function getznodedata(path, cb) {
	zkClient.getData(
	    path,
	    function (event) {
	    	// watch for path changes
			getznodedata(path, cb)
	    },
	    function (error, data, stat) {
	        if (error) {
	        	return cb(error)
	        }

	        return cb(null, data.toString('utf8'))
	    }
	);
}

function getznodedatanowatch(path, cb) {
	zkClient.getData(
	    path,
	    null,
	    function (error, data, stat) {
	        if (error) {
	        	return cb(error)
	        }

	        return cb(null, data.toString('utf8'))
	    }
	);
}

function removeznode(path, cb) {
	zkClient.remove(
		path, 
		// version,
		function (error) {
		    if (error) {
		    	return cb(error)
		    }

		    cb(null, util.format('%s was removed', path))
		}
	)
}

function listchildren(path, key, store, recurse, next, cb) {
    zkClient.getChildren(
        path,
        function (event) {
        	// watch /path for changes
        	listchildren(path, store, recurse, next, cb)
        },
        function (error, children, stat) {
            if (error) {
            	return cb(error)
            }

            next(path, key, children, store, recurse, next, cb)
        }
        
    );
}


var childqueue = []
var all = {}
// builds the list object
function loadchildren(path, key, children, store, recurse, next, cb) {
	var child, qchild, parentnode = {} 
	var znode = {}
	var haschildren = false
	var rootpath = path

	while (child = children.shift()) {
		var zchild = {}
		zchild.value = null
		zchild.parent = rootpath
		zchild.key = child
 		zchild.path = path === '/' ? path + child : path + '/' + child;	
 		zchild.nodes = {}
	 	znode[child] = zchild
		
		childqueue.push(zchild)			
		haschildren = true
	}

	parentnode[path] = znode
	_.extend( store, parentnode )

		
	if (recurse == true ) {
	
		// while (qchild = childqueue.shift()) {
		// 	console.log(qchild)
		// 	setTimeout(listchildren, 15, qchild.path, qchild.key, qchild, recurse, next, cb)
		// }

		_.each(store[path], function(child) {
			listchildren(child.path, child.key, child, recurse, next, cb)
		})

	} else {
		cb (null, store)
	}
}

function processQueue(cb) {



}




