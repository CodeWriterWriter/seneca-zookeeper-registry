"use strict";
// mocha zookeeper-registry.test.js

var util   = require('util')
var assert = require('assert')

var seneca = require('seneca')

describe('seneca-zookeeper-registry', function(){
  var si = seneca({log: 'silent'})

  si
    .use('zookeeper-registry', {server: '127.0.0.1', port: '2181'})
    .ready()
 
  it('create the key with a value', function(fin) {
     var expected = '/k1 created with value v1'

    si
      .act(
        {role:'seneca-zookeeper-registry',cmd:'create'}, 
        {key:'/k1', value:'v1'  }, 
        function(error, result) {
          if (error) { fin(error) } 
          assert.equal(result, expected)
          fin()
        }
      )

  })

  it('get the keys value', function(fin) {
    // var si = seneca({log: 'silent'})
    var expected = 'v1'

    si
      .act(
        {role:'seneca-zookeeper-registry',cmd:'get'}, 
        {key:'/k1'}, 
        function (error, result) {
          if (error) { fin(error) }
          assert.equal(result, expected)
          fin()
        }
      )
  })


  it('list the keys & values', function(fin) {
    // var si = seneca({log: 'silent'})
    var expected =  {
      '/k1': {}
    }

    si
      .act(
        {role:'seneca-zookeeper-registry',cmd:'list'}, 
        {key:'/k1'}, 
        function (error, result) {
          if (error) { fin(error) }
          assert.equal(result, expected)
          fin()
        }
      )

    fin()
  })

  it('remove the key', function(fin) {
    // var si = seneca({log: 'silent'})
    var expected =  '/k1 was removed'

    si
      .act(
        {role:'seneca-zookeeper-registry',cmd:'remove'}, 
        {key:'/k1'}, 
        function (error, result) {
          if (error) { fin(error) }
          assert.equal(result, expected)
          fin()
        }
      )

    fin()
  })

})