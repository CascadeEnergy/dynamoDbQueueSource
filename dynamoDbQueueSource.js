'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');

var dynamoDb = new AWS.DynamoDB();

var dynamoDbQueueSource = { };

dynamoDbQueueSource._processResult = function(err, data, task) {
  // Did we have an error? If so, pass it through to the callback, indicate
  // that we should stop iterating, and return
  if (err) {
    task.error = err;
    task.callback(err);
    return;
  }

  // Push the items from the scan into the queue and update the total item count
  task.queue.push(data.Items);
  task.results.itemCount += data.Items.length;

  // Are we done? If so, trigger the callback for the scan and return
  if (!data.LastEvaluatedKey) {
    task.hasMoreItems = false;
    task.callback(null, task.results);
    return;
  }

  // If we have more rows to scan, repeat the scan with the new exclusive
  // start key
  task.options.ExclusiveStartKey = data.LastEvaluatedKey;
  setImmediate(task.executor);
};

dynamoDbQueueSource._executeScanTask = function(scanTask) {
  dynamoDb.scan(scanTask.options, scanTask.processor);
};

dynamoDbQueueSource._executeQueryTask = function(queryTask) {
  dynamoDb.query(queryTask.options, queryTask.processor);
};

dynamoDbQueueSource._isRunning = function(scanTask) {
  return (
    !scanTask.error &&
    (!scanTask.queue.idle() || scanTask.hasMoreItems)
  );
};

dynamoDbQueueSource._createTask = function(queue, options, callback, executor) {
  var task = {
    queue: queue,
    options: options,
    callback: callback,
    hasMoreItems: true,
    results: { itemCount: 0 }
  };

  // The callback for scanToQueue is optional, but everything else is simpler
  // if we ensure that a function is always available as the callback, so we
  // just use the identity function from lodash if we didn't get a real
  // callback function
  if (!_.isFunction(task.callback)) {
    task.callback = _.identity;
  }

  task.processor = _.partialRight(dynamoDbQueueSource._processResult, task);
  task.executor = _.partialRight(executor, task);
  task.isRunning = _.partial(dynamoDbQueueSource._isRunning, task);

  return task;
};

dynamoDbQueueSource.scanToQueue = function(queue, options, callback) {
  var scanTask = dynamoDbQueueSource._createTask(
    queue,
    options,
    callback,
    dynamoDbQueueSource._executeScanTask
  );

  setImmediate(scanTask.executor);

  return scanTask;
};

dynamoDbQueueSource.queryToQueue = function(queue, options, callback) {
  var queryTask = dynamoDbQueueSource._createTask(
    queue,
    options,
    callback,
    dynamoDbQueueSource._executeQueryTask
  );

  setImmediate(queryTask.executor);

  return queryTask;
};

module.exports = dynamoDbQueueSource;
