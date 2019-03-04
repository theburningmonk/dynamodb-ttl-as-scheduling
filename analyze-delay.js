const _ = require('lodash')
const AWS = require('./functions/lib/aws')
AWS.config.region = 'us-east-1'
const DynamoDB = new AWS.DynamoDB.DocumentClient()

const getDelays = async () => {
  const delays = []
  const loop = async (startKey) => {
    const req = {
      TableName: 'scheduled_item_results',
      ExclusiveStartKey: startKey
    }
    const resp = await DynamoDB.scan(req).promise()
    const newDelays = resp.Items.map(x => {
      const scheduled = new Date(x.scheduledFor)
      const executed = new Date(x.executedAt)
      return executed.getTime() - scheduled.getTime()
    })
    // console.log(`new delays: ${newDelays}`)

    delays.push(...newDelays)

    if (resp.LastEvaluatedKey) {
      console.log('recursing...')
      return loop(resp.LastEvaluatedKey)
    }
  }

  await loop()
  return delays
}

const calcStats = (delays) => {
  console.log(`total datapoints: ${delays.length}`)

  const average = (_.sum(delays) / delays.length) / 1000 / 60
  console.log(`average delay is ${average}mins`)

  const min = _.min(delays) / 1000 / 60
  console.log(`min delay is ${min}mins`)

  const max = _.max(delays) / 1000 / 60
  console.log(`max delay is ${max}mins`)

  const sorted = _.sortBy(delays)
  const median = sorted[delays.length / 2] / 1000 / 60
  console.log(`median delay is ${median}mins`)

  const ninetieth = sorted[Math.ceil(delays.length * 0.9)] / 1000 / 60
  console.log(`90th percentile is ${ninetieth}mins`)
}

getDelays().then(calcStats)
