'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');

var dynamoDb = new AWS.DynamoDB();

var dynamoDbQueueSource = { };

dynamoDbQueueSource._processScanResult = function(err, data, scanTask) {
  // Did we have an error? If so, pass it through to the callback, indicate
  // that we should stop iterating, and return
  if (err) {
    scanTask.error = err;
    scanTask.callback(err);
    return;
  }

  // Push the items from the scan into the queue and update the total item count
  scanTask.queue.push(data.Items);
  scanTask.results.itemCount += data.Items.length;

  // Are we done? If so, trigger the callback for the scan and return
  if (!data.LastEvaluatedKey) {
    scanTask.hasMoreItems = false;
    scanTask.callback(null, scanTask.results);
    return;
  }

  // If we have more rows to scan, repeat the scan with the new exclusive
  // start key
  scanTask.options.ExclusiveStartKey = data.LastEvaluatedKey;
  setImmediate(_.partial(dynamoDbQueueSource._executeScanTask, scanTask));
};

dynamoDbQueueSource._executeScanTask = function(scanTask) {
  dynamoDb.scan(scanTask.options, scanTask.processor);
};

dynamoDbQueueSource._isRunning = function(scanTask) {
  return (
  !scanTask.error &&
  (scanTask.queue.length > 0 || scanTask.hasMoreItems)
  );
};

dynamoDbQueueSource.scanToQueue = function(queue, options, callback) {
  var scanTask = {
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
  if (!_.isFunction(scanTask.callback)) {
    scanTask.callback = _.identity;
  }

  scanTask.processor = _.partialRight(
    dynamoDbQueueSource._processScanResult,
    scanTask
  );

  scanTask.isRunning = _.partial(dynamoDbQueueSource._isRunning, scanTask);

  dynamoDbQueueSource._executeScanTask(scanTask);

  return scanTask;
};

module.exports = dynamoDbQueueSource;
