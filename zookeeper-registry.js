"use strict";

var util = require('util')
var zookeeper = require('node-zookeeper-client')
var zkClient
var _ = require('underscore')

module.exports = function(opts) {
	var seneca = this
	var plugin = 'seneca-zookeeper-registry'
	var store
	var stack

	seneca.add( {role:plugin, cmd:'create'}, cmd_create)
	seneca.add( {role:plugin, cmd:'set'},    cmd_set)
	seneca.add( {role:plugin, cmd:'get'},    cmd_get)
	seneca.add( {role:plugin, cmd:'list'},   cmd_list)
	seneca.add( {role:plugin, cmd:'remove'}, cmd_remove)

	seneca.add( {init:plugin}, function(args, done) {
		store = {}
		stack = {}
		
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

		getchildren(args.key, stack, recurse, loadchildren, function(error, stack) {
			if (error) { cb(error) }

			buildStore(stack, done)

		})

	}

	function cmd_remove( args, done ) {
		removeznode(args.key, done)
	}

	return {
		name: plugin
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

			    	return cb(null, util.format('%s created with value %s', path, value))	
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

	        return cb(null, path, data.toString('utf8'))
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

function listchildren(path, stack, cb) {
    zkClient.getChildren(
        path,
        function (event) {
        	// watch /path for changes
        	listchildren(path, stack, cb)
        },
        function (error, children, stat) {
            if (error) {
            	return cb(error)
            }

            cb(null, path, stack, children)
        }
        
    );
}



// builds the list object
function getchildren(path, stack, recurse, next, cb) {
	var child, qchild
	var parentnode = {}, znode = {}, zchild = {}
	var childqueue = []

	var processChildren = function(error, path, stack, children) {
		/*	find the child in the stack and see who is its parent, then decrement the queue */
		_.each(stack, function(value, key) {
			// do we have this path in the stack?
			if (value.path == path) {
				_.each(childqueue, function(qchild) {
					// do we have the parent path in the stack?
					if (qchild.path == value.parent) {
						if (qchild.num > 0) {
							qchild.num--

							if (qchild.num == 0) {
								// once we've processed all children remove from queue
								childqueue = _.reject(childqueue, function(del) {
									return del === qchild
								})
							}
						}
					}// qchild.path
				})// each
			}// value.path
		})// each

		if (children.length > 0) {
			parentnode = {
				path: path,
				num: children.length
			}
			/* add to queue */
			childqueue.push(parentnode)

			while (child = children.shift()) {
				var zchild = {}
				zchild.value = null
				zchild.parent = path
				zchild.key = child
				zchild.path = path === '/' ? path + child : path + '/' + child;	
				znode[child] = zchild
				_.extend(stack, znode)

				listchildren(zchild.path, stack, processChildren)
			}			
		} else if (childqueue.length > 0) {
			//still processing children
		} else {
			/* done with gathering children, need to gather values and then flatten*/
			next(stack, cb)
		}
	}
	
	listchildren(path, stack, processChildren)
}

/* takes the store object and gets values for all its children */
function loadchildren(store, cb) {
	var child
	var loadqueue = []

	var processValue = function(error, path, result) {
		_.each(store, function(value, key) {
			if (value.path == path) {
				/* remove from queue */
				loadqueue = _.reject(loadqueue, function(del) {
					return del === value
				})

				value.value = result
			}
		})

		if (loadqueue.length == 0) {
			cb(null, store)
		}
	}

	/* load queue and get data */
	_.each(store, function(value, key) {
		if (value.path) {
			loadqueue.push(value)
			getznodedatanowatch(value.path,  processValue)			
		}
	})
}

function buildStore(store, cb) {
	console.log('buildStore')

	cb(null, store)
}

