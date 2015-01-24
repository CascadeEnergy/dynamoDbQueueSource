// jshint -W030

'use strict';

var _ = require('lodash');
var proxyquire = require('proxyquire');
var sinon = require('sinon');
var should = require('should');

describe('Library: Location Queue Source', function() {
  var dynamoDb;
  var dynamoDbQueueSource;
  var dynamoDbQueueSourceFactory;
  var lodash;

  beforeEach(function() {
    dynamoDb = { query: sinon.stub(), scan: sinon.stub() };
    lodash = {
      identity: sinon.spy(),
      partial: sinon.stub(),
      partialRight: sinon.stub()
    };

    dynamoDbQueueSourceFactory = proxyquire(
      '../dynamoDbQueueSource.js',
      {
        lodash: lodash
      }
    );

    dynamoDbQueueSource = dynamoDbQueueSourceFactory(dynamoDb);
  });

  describe('Query / Scan interface', function() {
    var clock;

    beforeEach(function() {
      clock = sinon.useFakeTimers();
    });

    afterEach(function() {
      clock.restore();
    });

    describe('scanToQueue', function() {
      it('should prepare a scan task and begin executing it', function() {
        var task = { 'executor': sinon.spy() };
        var callback = sinon.spy();

        dynamoDbQueueSource._createTask = sinon.stub().returns(task);

        should(
          dynamoDbQueueSource.scanToQueue('queue', 'options', callback)
        ).equal(task);

        dynamoDbQueueSource._createTask.callCount.should.equal(1);
        should(dynamoDbQueueSource._createTask.args[0][0]).equal('queue');
        should(dynamoDbQueueSource._createTask.args[0][1]).equal('options');
        should(dynamoDbQueueSource._createTask.args[0][2]).equal(callback);
        should(dynamoDbQueueSource._createTask.args[0][3]).equal(
          dynamoDbQueueSource._executeScanTask
        );

        clock.tick();

        task.executor.callCount.should.equal(1);
      });
    });

    describe('queryToQueue', function() {
      it('should prepare a query task and begin executing it', function() {
        var task = { 'executor': sinon.spy() };
        var callback = sinon.spy();

        dynamoDbQueueSource._createTask = sinon.stub().returns(task);

        should(
          dynamoDbQueueSource.queryToQueue('queue', 'options', callback)
        ).equal(task);

        dynamoDbQueueSource._createTask.callCount.should.equal(1);
        should(dynamoDbQueueSource._createTask.args[0][0]).equal('queue');
        should(dynamoDbQueueSource._createTask.args[0][1]).equal('options');
        should(dynamoDbQueueSource._createTask.args[0][2]).equal(callback);
        should(dynamoDbQueueSource._createTask.args[0][3]).equal(
          dynamoDbQueueSource._executeQueryTask
        );

        clock.tick();

        task.executor.callCount.should.equal(1);
      });
    });
  });

  describe('DynamoDB delegation' ,function() {
    var task = { options: 'options', processor: 'processor' };

    describe('_executeScanTask', function() {
      it('should delegate execution to the DynamoDB client', function() {
        dynamoDbQueueSource._executeScanTask(task);

        dynamoDb.scan.callCount.should.equal(1);
        should(dynamoDb.scan.args[0][0]).equal(task.options);
        should(dynamoDb.scan.args[0][1]).equal(task.processor);
      });
    });

    describe('_executeQueryTask', function() {
      it('should delegate execution to the DynamoDB client', function() {
        dynamoDbQueueSource._executeQueryTask(task);

        dynamoDb.query.callCount.should.equal(1);
        should(dynamoDb.query.args[0][0]).equal(task.options);
        should(dynamoDb.query.args[0][1]).equal(task.processor);
      });
    });
  });

  describe('_createTask', function() {
    var callback;
    var executor;

    beforeEach(function() {
      callback = sinon.spy();
      executor = sinon.spy();

      lodash.partialRight.onFirstCall().returns('process-result');
      lodash.partialRight.onSecondCall().returns('executor');
      lodash.partial.onFirstCall().returns('is-running');
    });

    it('should populate a basic task object', function() {
      var result;

      result = dynamoDbQueueSource._createTask(
        'queue',
        'options',
        callback,
        executor
      );

      should(result).eql(
        {
          queue: 'queue',
          options: 'options',
          callback: callback,
          hasMoreItems: true,
          results: { itemCount: 0 },
          processor: 'process-result',
          executor: 'executor',
          isRunning: 'is-running'
        }
      );

      lodash.partialRight.callCount.should.equal(2);
      should(lodash.partialRight.args[0][0]).equal(
        dynamoDbQueueSource._processResult
      );
      should(lodash.partialRight.args[0][1]).equal(result);
      should(lodash.partialRight.args[1][0]).equal(executor);
      should(lodash.partialRight.args[1][1]).equal(result);

      lodash.partial.callCount.should.equal(1);
      should(lodash.partial.args[0][0]).equal(
        dynamoDbQueueSource._isRunning
      );
      should(lodash.partial.args[0][1]).equal(result);
    });

    it('should provide a default callback if necessary', function() {
      var result;

      result = dynamoDbQueueSource._createTask(
        'queue',
        'options',
        null,
        executor
      );

      should(result.callback).equal(lodash.identity);
    });
  });

  describe('_processResult', function() {
    var clock;
    var scanTask;

    before(function () { clock = sinon.useFakeTimers(); });
    after(function () { clock.restore(); });

    beforeEach(function() {
      scanTask = {
        callback: sinon.stub(),
        options: { },
        results: { itemCount: 0 },
        queue: { push: sinon.spy() },
        executor: sinon.spy()
      };
    });

    it('should add the scanned items to a queue', function() {
      var data = { Items: [ 1, 2, 3 ] };

      dynamoDbQueueSource._processResult(null, data, scanTask);

      scanTask.queue.push.callCount.should.equal(1);
      should(scanTask.queue.push.args[0][0]).equal(data.Items);
      should(scanTask.results.itemCount).equal(3);
    });

    it('should trigger the callback when there are no more items', function() {
      var data = { Items: 'items' };

      dynamoDbQueueSource._processResult(null, data, scanTask);

      scanTask.callback.callCount.should.equal(1);
      should(scanTask.callback.args[0][0]).not.exist;
    });

    it('should trigger the callback on an error', function() {
      dynamoDbQueueSource._processResult('error', { }, scanTask);

      scanTask.queue.push.callCount.should.equal(0);

      scanTask.callback.callCount.should.equal(1);
      should(scanTask.callback.args[0][0]).equal('error');
    });

    it('should continue the scan if there are more results', function() {
      var data = { LastEvaluatedKey: 'last-key', Items: [ 1, 2, 3 ] };
      var executeScanTask = sinon.spy();

      lodash.partial.returns(executeScanTask);

      sinon.spy(dynamoDbQueueSource, '_executeScanTask');

      dynamoDbQueueSource._processResult(null, data, scanTask);

      clock.tick();

      should(scanTask.options.ExclusiveStartKey).equal('last-key');

      scanTask.executor.callCount.should.equal(1);
    });
  });

  describe('_isRunning', function() {
    it('should return true if the queue is not idle', function() {
      var scanTask = { queue: { idle: sinon.stub() }, hasMoreItems: false };
      scanTask.queue.idle.returns(false);

      dynamoDbQueueSource._isRunning(scanTask).should.be.true;
    });

    it('should return true if scanning has not finished', function() {
      var scanTask = { queue: { idle: sinon.stub() }, hasMoreItems: true };
      scanTask.queue.idle.returns(true);

      dynamoDbQueueSource._isRunning(scanTask).should.be.true;
    });

    it(
      'should return false if scanning has finished and the queue is idle',
      function() {
        var scanTask = {
          hasMoreItems: false,
          queue: { idle: sinon.stub() }
        };
        scanTask.queue.idle.returns(true);

        dynamoDbQueueSource._isRunning(scanTask).should.be.false;
      }
    );
  });
});
