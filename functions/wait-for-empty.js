const _ = require('lodash')
const AWS = require('./lib/aws')
const DynamoDB = new AWS.DynamoDB.DocumentClient()

const { TABLE_NAME } = process.env

module.exports.handler = async (event, context) => {
  console.log(`waiting for ${TABLE_NAME} to be empty...`)
  const req = {
    TableName : TABLE_NAME,
    Limit: 1    
  }
  const resp = await DynamoDB.scan(req).promise()
  return {
    isEmpty: _.isEmpty(resp.Items)
  }  
}