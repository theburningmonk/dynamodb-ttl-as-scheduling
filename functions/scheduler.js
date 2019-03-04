const AWS = require('./lib/aws')
const DynamoDB = new AWS.DynamoDB.DocumentClient()

const { TABLE_NAME, RESULT_TABLE_NAME } = process.env

module.exports.handler = async (event, context) => {
  const { id, execute_at } = JSON.parse(event.body)
  const ttl = Math.ceil(new Date(execute_at).getTime() / 1000)

  const req = {
    TransactItems: [{
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
          scheduledFor: new Date(execute_at).toJSON()
        }
      }
    }]    
  }
  await DynamoDB.transactWrite(req).promise()

  return {
    statusCode: 200,
    body: JSON.stringify({ id, ttl })
  }
}