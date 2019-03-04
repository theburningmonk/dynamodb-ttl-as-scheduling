const uuid = require('uuid/v4')
const _ = require('lodash')
const AWS = require('./lib/aws')
const DynamoDB = new AWS.DynamoDB.DocumentClient()
const ONE_MIN = 60 * 1000

const { TABLE_NAME, RESULT_TABLE_NAME } = process.env

module.exports.handler = async (event, context) => {
  const { count, minDelayMins, maxDelayMins } = event
  if (!_.isInteger(count) || 
      !_.isInteger(minDelayMins) || 
      !_.isInteger(maxDelayMins)) {
    throw new Error('expecting { count, minDelayMins, maxDelayMins }')
  }

  await schedule(count, minDelayMins, maxDelayMins)
}

function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max));
}

function addOne(executeAt) {
  const id = uuid()
  const ttl = Math.ceil(new Date(executeAt).getTime() / 1000)
  return [{
    Put: {
      TableName: TABLE_NAME,
      Item: {
        id,
        ttl
      }
    }
  }, {
    Put: {
      TableName: RESULT_TABLE_NAME,
      Item: {
        id,
        scheduledFor: new Date(executeAt).toJSON()
      }
    }
  }]
}

async function schedule(count, minDelayMins, maxDelayMins) {
  var transactItems = []  
  for (var i = 0; i < count; i++) {
    const now = Date.now()
    const delay = ONE_MIN * minDelayMins + getRandomInt(ONE_MIN * maxDelayMins)
    const executeAt = now + delay
    const newTransactItems = addOne(executeAt)

    transactItems.push(...newTransactItems)
    if (transactItems.length >= 10) {
      const req = {
        TransactItems: transactItems
      }
      await DynamoDB.transactWrite(req).promise()
      transactItems = []
    }
  }

  if (transactItems.length > 0) {
    const req = {
      TransactItems: transactItems
    }
    await DynamoDB.transactWrite(req).promise()
  }

  console.log(`scheduled ${count} items`)
}
