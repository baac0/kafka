import { strictEqual, ok, deepStrictEqual, partialDeepStrictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test, type TestContext } from 'node:test'

import { type FetchResponse } from '../../../src/apis/consumer/fetch-v17.ts'

import {
  Consumer,
  type ConsumerOptions,
  type Deserializers,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  type Serializers,
  stringDeserializers,
  stringSerializers,
  type MessageToProduce,
  sleep
} from '../../../src/index.ts'
import { createTopic } from '../../helpers.ts'

const kafkaBootstrapServers = ['localhost:9092']

// Helper functions
function createTestGroupId () {
  return `test-consumer-group-${randomUUID()}`
}

function createProducer<K = string, V = string, HK = string, HV = string> ({
  t,
  options
}: {
  t: TestContext,
  options?: Partial<ProducerOptions<K, V, HK, HV>>
}): Producer<K, V, HK, HV> {
  options ??= {}

  const producer = new Producer({
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    serializers: stringSerializers as Serializers<K, V, HK, HV>,
    autocreateTopics: false,
    ...options
  })

  t.after(() => producer.close())

  return producer
}

// This helper creates a consumer and ensure proper termination
function createConsumer<K = string, V = string, HK = string, HV = string> ({
  t,
  groupId,
  options
}: {
  t: TestContext,
  groupId?: string,
  options?: Partial<ConsumerOptions<K, V, HK, HV>>
}): Consumer<K, V, HK, HV> {
  options ??= {}

  const consumer = new Consumer<K, V, HK, HV>({
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: groupId ?? createTestGroupId(),
    deserializers: stringDeserializers as Deserializers<K, V, HK, HV>,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...options
  })

  t.after(async () => {
    await consumer.close(true)
  })

  return consumer
}

// This function produces sample messages to a topic for testing the consumer
async function produceTestMessages ({
  t,
  messages,
  batchSize = 3,
  delay = 0,
  options
}: {
  t: TestContext,
  messages: MessageToProduce<string, string, string, string>[],
  batchSize?: number,
  delay?: number,
  options?: Partial<ProducerOptions<string, string, string, string>>
}): Promise<void> {
  const producer = createProducer({ t, options })

  for (let i = 0; i < messages.length; i += batchSize) {
    await producer.send({
      messages: messages.slice(i, i + batchSize),
      acks: ProduceAcks.LEADER
    })
    await sleep(delay)
  }
}

async function fetchFromOffset ({
  consumer,
  topic,
  fetchOffset = 0n
}: {
  consumer: Consumer<string, string, string, string>,
  topic: string,
  fetchOffset: bigint
}): Promise<FetchResponse> {
  const metadata = await consumer.metadata({ topics: [topic] })
  const { id: topicId, partitions } = metadata.topics.get(topic)!
  const partition = partitions[0]
  return consumer.fetch({
    node: partition.leader,
    topics: [
      {
        topicId,
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: partition.leaderEpoch,
            fetchOffset,
            lastFetchedEpoch: -1,
            partitionMaxBytes: 1048576 // Default max bytes per partition
          }
        ]
      }
    ]
  })
}

test('fetch should retrieve messages', { only: true }, async t => {
  const groupId = createTestGroupId()
  const topic = await createTopic(t, true)
  const count = 6
  const batchSize = 3

  const messages = Array.from({ length: count }, (_, i) => ({
    topic,
    key: `key-${i}`,
    value: `value-${i}`,
    headers: { headerKey: `headerValue-${i}` }
  }))

  // Produce test messages
  await produceTestMessages({ t, messages, batchSize, delay: 100 })

  const consumer = createConsumer({ t, groupId })
  const fetchResult = await fetchFromOffset({
    consumer,
    topic,
    fetchOffset: 0n
  })

  strictEqual(fetchResult.errorCode, 0, 'Should succeed fetching')
  strictEqual(fetchResult.responses.length, 1, 'Should return one topic')
  strictEqual(fetchResult.responses[0].partitions.length, 1, 'Should return one partition')
  const fetchPartition = fetchResult.responses[0].partitions[0]!
  strictEqual(fetchPartition.errorCode, 0, 'Should succeed fetching partition')
  partialDeepStrictEqual(fetchPartition.records, {
    attributes: 0,
    magic: 2,
    firstOffset: 0n,
    lastOffsetDelta: batchSize - 1,
    length: 178
  }, 'Should return records batch with correct metadata')
  const records = fetchPartition.records!.records
  strictEqual(records.length, batchSize, 'Should get all messages in batch')

  const fetchMessages = records.map((record, i) => {
    strictEqual(record.offsetDelta, i, 'Should have correct offset delta for each record')
    return {
      topic,
      key: record.key.toString('utf-8'),
      value: record.value.toString('utf-8'),
      headers: Object.fromEntries(record.headers.map(([key, value]) => [key.toString('utf-8'), value.toString('utf-8')]))
    }
  })
  deepStrictEqual(fetchMessages, messages.slice(0, batchSize), 'Should match produced messages in first batch')

  const fetchResultNextBatch = await fetchFromOffset({
    consumer,
    topic,
    fetchOffset: 3n
  })
  strictEqual(fetchResult.errorCode, 0, 'Should succeed fetching next batch')
  const fetchPartitionNextBatch = fetchResultNextBatch.responses[0].partitions[0]!
  strictEqual(fetchPartitionNextBatch.errorCode, 0, 'Should succeed fetching partition next batch')
  ok(fetchPartitionNextBatch.records!.firstTimestamp > fetchPartition.records!.firstTimestamp, 'Should have later timestamp in next batch')
  partialDeepStrictEqual(fetchPartitionNextBatch.records, {
    attributes: 0,
    magic: 2,
    firstOffset: 3n,
    lastOffsetDelta: batchSize - 1,
    length: 178
  }, 'Should return records next batch with correct metadata')

  const recordsNextBatch = fetchPartitionNextBatch.records!.records
  const fetchMessagesNextBatch = recordsNextBatch.map((record, i) => {
    strictEqual(record.offsetDelta, i, 'Should have correct offset delta for each record')
    return {
      topic,
      key: record.key.toString('utf-8'),
      value: record.value.toString('utf-8'),
      headers: Object.fromEntries(record.headers.map(([key, value]) => [key.toString('utf-8'), value.toString('utf-8')]))
    }
  })
  deepStrictEqual(fetchMessagesNextBatch, messages.slice(batchSize, count), 'Should match produced messages in first batch')
})

test('fetch should handle empty key/headers', { only: true }, async t => {
  const groupId = createTestGroupId()
  const topic = await createTopic(t, true)
  const count = 6
  const batchSize = 3

  const messages = Array.from({ length: count }, (_, i) => ({
    topic,
    value: `value-${i}`
  }))

  // Produce test messages
  await produceTestMessages({ t, messages, batchSize, delay: 100 })

  const consumer = createConsumer({ t, groupId })
  const fetchResult = await fetchFromOffset({
    consumer,
    topic,
    fetchOffset: 0n
  })

  strictEqual(fetchResult.errorCode, 0, 'Should succeed fetching')
  strictEqual(fetchResult.responses.length, 1, 'Should return one topic')
  strictEqual(fetchResult.responses[0].partitions.length, 1, 'Should return one partition')
  const fetchPartition = fetchResult.responses[0].partitions[0]!
  strictEqual(fetchPartition.errorCode, 0, 'Should succeed fetching partition')
  partialDeepStrictEqual(fetchPartition.records, {
    attributes: 0,
    magic: 2,
    firstOffset: 0n,
    lastOffsetDelta: batchSize - 1
  }, 'Should return records batch with correct metadata')
  const records = fetchPartition.records!.records
  strictEqual(records.length, batchSize, 'Should get all messages in batch')

  const fetchMessages = records.map((record, i) => {
    strictEqual(record.offsetDelta, i, 'Should have correct offset delta for each record')
    return {
      topic,
      key: record.key.toString('utf-8'),
      value: record.value.toString('utf-8'),
      headers: Object.fromEntries(record.headers.map(([key, value]) => [key.toString('utf-8'), value.toString('utf-8')]))
    }
  })
  messages.slice(0, batchSize).forEach((message, i) => {
    partialDeepStrictEqual(fetchMessages[i], message, 'Should match produced messages in first batch')
  })
})
