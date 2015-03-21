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

		getchildren(args.key, stack, recurse, function(error, stack) {
			if (error) { done(error) }

			store[args.key] = {}
			rebuildStore(stack, store, args.key, done)
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

function listchildren(path, stack, recurse, cb) {
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

            cb(null, path, stack, recurse, children)
        }
        
    );
}

var cqueue = []
// build the stack from the path and then load all values
function getchildren(path, stack, recurse, cb) {

	function _gc(error, path, stack, recurse, children) {
		//if (error) { return cb(error) }

		var child
		var znode = {}, zchild = {}

		while (child = children.shift()) {
			var zchild = {}
			zchild.value = null
			zchild.parent = path
			zchild.key = child
			zchild.path = path === '/' ? path + child : path + '/' + child;	
			znode[child] = zchild
			_.extend(stack, znode)

			if (recurse == true) {
				cqueue.push(zchild.path)
				listchildren(zchild.path, stack, recurse, _gc)
			}
		}

		// remove from q when we are done listing children
		cqueue = _.reject(cqueue, function(del) { return del === path })

		if (cqueue.length == 0 ) {
			// finished processsing all children
			loadstackvalues(stack, cb)
		}
	}

	listchildren(path, stack, recurse, _gc)	
}

/* takes the stack and gets values for all items */
function loadstackvalues(stack, cb) {
	var child
	var loadqueue = []

	var processValue = function(error, path, result) {
		_.each(stack, function(value, key) {
			if (value.path == path) {
				/* remove from queue */
				loadqueue = _.reject(loadqueue, function(del) {
					return del === value
				})

				value.value = result
			}
		})

		if (loadqueue.length == 0) {
			cb(null, stack)
		}

	}

	/* load queue and get data */
	_.each(stack, function(value, key) {
		if (value.path) {
			loadqueue.push(value)
			getznodedatanowatch(value.path,  processValue)			
		}
	})
}

function _listchildren(stack, path) {
    var children = _.filter(stack, function(item) {
        if (item.parent == path) {
           return item
        }
    })

    return children
}

// build a root child
function _bc(parent, key, child) {
    var child_key = child.key
    delete child['path']
    delete child['parent']
    delete child['key']

    parent[key][child_key] = child
    return parent
}

// build a leaf child
function _bc2(parent, child) {
    parent[child.key] = { value: child.value }
    return parent
}

// q used for traversing children
var rbq = []
// traverse children
function rebuildStore(stack, parent, path, cb) {
    var child
    var c = _listchildren(stack, path)

    while ( child = c.shift() ) {
        // add child to the q while we gather leaf children
        rbq.push(child.path)

        var p = rebuildStore(stack, child, child.path, cb)

        // remove child from the q once we have all the children
        rbq = _.reject(rbq, function(item) { return item == p.path })

        if (parent.path) {
            // this is a child node coming back
            parent = _bc2(parent, p)
        } else {
            // need to add this to root
            parent = _bc(parent, path, p)
        }

        if (rbq.length == 0 && c.length == 0) {
            cb(null, parent)
        }
    }
    // end leaf
    return parent
}

