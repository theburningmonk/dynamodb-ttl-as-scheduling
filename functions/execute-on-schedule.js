const AWS = require('./lib/aws')
const DynamoDB = new AWS.DynamoDB.DocumentClient()

const { RESULT_TABLE_NAME } = process.env

module.exports.handler = async (event, context) => {
  const transactItems = event.Records
    .filter(x => x.eventName === 'REMOVE')
    .map(x => x.dynamodb.OldImage.id.S)
    .map(id => ({
      Update: {
        TableName: RESULT_TABLE_NAME,
        Key: {
          id
        },
        UpdateExpression: 'set executedAt = :now',
        ExpressionAttributeValues: {
          ':now': new Date().toJSON()
        }
      }
    }))

  if (transactItems.length === 0) {
    return
  }

  const req = {
    TransactItems: transactItems
  }
  await DynamoDB.transactWrite(req).promise()
}