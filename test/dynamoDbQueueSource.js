// jshint -W030

'use strict';

var _ = require('lodash');
var proxyquire = require('proxyquire');
var sinon = require('sinon');
var should = require('should');

describe('Library: Location Queue Source', function() {
  var aws;
  var dynamoDb;
  var dynamoDbQueueSource;
  var lodash;

  beforeEach(function() {
    aws = { DynamoDB: sinon.stub() };
    dynamoDb = { scan: sinon.stub() };
    lodash = {
      partial: sinon.stub(),
      partialRight: sinon.stub()
    };

    aws.DynamoDB.returns(dynamoDb);

    dynamoDbQueueSource = proxyquire(
      '../dynamoDbQueueSource.js',
      {
        'aws-sdk': aws,
        lodash: lodash
      }
    );
  });

  describe('scanToQueue', function() {
    var scanTask;

    beforeEach(function() {
      scanTask = {
        queue: 'queue',
        options: 'options',
        callback: sinon.spy(),
        results: { itemCount: 0 },
        processor: 'process-scan-result',
        hasMoreItems: true,
        isRunning: 'is-running'
      };

      lodash.partialRight.returns('process-scan-result');
      lodash.partial.returns('is-running');
      sinon.stub(dynamoDbQueueSource, '_executeScanTask');
    });

    it('should prepare a scan task and begin executing it', function() {
      dynamoDbQueueSource.scanToQueue(
        scanTask.queue,
        scanTask.options,
        scanTask.callback
      );

      lodash.partialRight.callCount.should.equal(1);
      should.strictEqual(
        lodash.partialRight.args[0][0],
        dynamoDbQueueSource._processScanResult
      );
      should(lodash.partialRight.args[0][1]).eql(scanTask);

      lodash.partial.callCount.should.equal(1);
      should.strictEqual(
        lodash.partial.args[0][0],
        dynamoDbQueueSource._isRunning
      );
      should(lodash.partial.args[0][1]).eql(scanTask);

      dynamoDbQueueSource._executeScanTask.callCount.should.equal(1);
      should(dynamoDbQueueSource._executeScanTask.args[0][0]).eql(scanTask);
    });

    it('should provide a default callback if required', function() {
      dynamoDbQueueSource.scanToQueue(scanTask.queue, scanTask.options);

      scanTask.callback = _.identity;
      dynamoDbQueueSource._executeScanTask.callCount.should.equal(1);
      should(dynamoDbQueueSource._executeScanTask.args[0][0]).eql(scanTask);
    });
  });

  describe('_executeScanTask', function() {
    it('should delegate execution to the DynamoDB client', function() {
      var scanTask = { options: 'options', processor: 'processor' };

      dynamoDbQueueSource._executeScanTask(scanTask);

      dynamoDb.scan.callCount.should.equal(1);
      should(dynamoDb.scan.args[0][0]).equal(scanTask.options);
      should(dynamoDb.scan.args[0][1]).equal(scanTask.processor);
    });
  });

  describe('_processScanResult', function() {
    var clock;
    var scanTask;

    before(function () { clock = sinon.useFakeTimers(); });
    after(function () { clock.restore(); });

    beforeEach(function() {
      scanTask = {
        callback: sinon.stub(),
        options: { },
        results: { itemCount: 0 },
        queue: { push: sinon.spy() }
      };
    });

    it('should add the scanned items to a queue', function() {
      var data = { Items: [ 1, 2, 3 ] };

      dynamoDbQueueSource._processScanResult(null, data, scanTask);

      scanTask.queue.push.callCount.should.equal(1);
      should(scanTask.queue.push.args[0][0]).equal(data.Items);
      should(scanTask.results.itemCount).equal(3);
    });

    it('should trigger the callback when there are no more items', function() {
      var data = { Items: 'items' };

      dynamoDbQueueSource._processScanResult(null, data, scanTask);

      scanTask.callback.callCount.should.equal(1);
      should(scanTask.callback.args[0][0]).not.exist;
    });

    it('should trigger the callback on an error', function() {
      dynamoDbQueueSource._processScanResult('error', { }, scanTask);

      scanTask.queue.push.callCount.should.equal(0);

      scanTask.callback.callCount.should.equal(1);
      should(scanTask.callback.args[0][0]).equal('error');
    });

    it('should continue the scan if there are more results', function() {
      var data = { LastEvaluatedKey: 'last-key', Items: [ 1, 2, 3 ] };
      var executeScanTask = sinon.spy();

      lodash.partial.returns(executeScanTask);

      sinon.spy(dynamoDbQueueSource, '_executeScanTask');

      dynamoDbQueueSource._processScanResult(null, data, scanTask);
      clock.tick();

      should(scanTask.options.ExclusiveStartKey).equal('last-key');

      lodash.partial.callCount.should.equal(1);
      should(lodash.partial.args[0][0]).equal(
        dynamoDbQueueSource._executeScanTask
      );
      should(lodash.partial.args[0][1]).equal(scanTask);

      executeScanTask.callCount.should.equal(1);
    });
  });

  describe('_isRunning', function() {
    it('should return true if the queue has items', function() {
      var scanTask = { queue: [ 1, 2, 3 ], hasMoreItems: false };

      dynamoDbQueueSource._isRunning(scanTask).should.be.true;
    });

    it('should return true if scanning has not finished', function() {
      var scanTask = { queue: [ ], hasMoreItems: true };

      dynamoDbQueueSource._isRunning(scanTask).should.be.true;
    });

    it(
      'should return false if scanning has finished and the queue is empty',
      function() {
        var scanTask = {
          hasMoreItems: false,
          queue: [ ]
        };

        dynamoDbQueueSource._isRunning(scanTask).should.be.false;
      }
    );
  });
});
