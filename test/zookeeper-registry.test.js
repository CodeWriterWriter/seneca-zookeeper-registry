"use strict";

// mocha zookeeper-registry.test.js

var util   = require('util')
var assert = require('assert')

var _   = require('lodash')
var seneca = require('seneca')


describe('plugin', function(){

  it('getset', function(fin) {
    var si = seneca({log:'silent'})
    si.use('..')
    var store = si.export('registry/store')

    si
        .start(fin)
        
        .wait('role:registry,cmd:set,key:k1,value:v1')
        .step(function(){
            assert.deepEqual(store(), { k1: { '$': 'v1' } })
            return true;
        })

      // .wait('role:registry,cmd:get,key:k1')
      // .step(function(data){
      //   assert('v1'==data.value)
      //   assert.deepEqual(store(), { k1: { '$': 'v1' } })
      //   return true;
      // })

      .end()
  })
})