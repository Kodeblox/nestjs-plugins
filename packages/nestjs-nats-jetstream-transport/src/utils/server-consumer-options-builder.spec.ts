import { serverConsumerOptionsBuilder } from './server-consumer-options-builder';
import * as nats from 'nats';

jest.mock('nats', () => {
  return {
    consumerOpts: jest.fn().mockImplementation(() => {
      const mockOpts = {};
      const chainableMethods = [
        'deliverGroup',
        'ackWait',
        'manualAck',
        'ackAll',
        'ackExplicit',
        'ackNone',
        'deliverAll',
        'deliverLast',
        'deliverLastPerSubject',
        'deliverNew',
        'deliverTo',
        'description',
        'durable',
        'filterSubject',
        'flowControl',
        'headersOnly',
        'idleHeartbeat',
        'limit',
        'maxAckPending',
        'maxDeliver',
        'maxMessages',
        'maxWaiting',
        'orderedConsumer',
        'replayInstantly',
        'replayOriginal',
        'sample',
        'startAtTimeDelta',
        'startSequence',
        'startTime',
      ];
      for (const method of chainableMethods) {
        mockOpts[method] = jest.fn().mockReturnValue(mockOpts);
      }
      return mockOpts;
    }),
    createInbox: jest.fn().mockReturnValue('mockInbox'),
  };
});

describe('serverConsumerOptionsBuilder', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should apply initial config with new JetStream properties like backoff and filterSubjects', () => {
    serverConsumerOptionsBuilder(
      {
        backoff: [1000, 5000],
        filterSubjects: ['events.A', 'events.B'],
        inactiveThreshold: 20000,
        memStorage: true,
      },
      'test.subject',
    );

    expect(nats.consumerOpts).toHaveBeenCalledWith({
      backoff: [1000, 5000],
      filter_subjects: ['events.A', 'events.B'],
      inactive_threshold: 20000,
      mem_storage: true,
    });
  });

  it('should still map standard builder properties correctly', () => {
    const builder = serverConsumerOptionsBuilder(
      {
        durable: 'my-durable',
        maxDeliver: 5,
        ackPolicy: 'Explicit',
      },
      'test.subject',
    );

    expect(nats.consumerOpts).toHaveBeenCalledWith({});
    expect(builder.durable).toHaveBeenCalledWith('my-durable-test_subject');
    expect(builder.maxDeliver).toHaveBeenCalledWith(5);
    expect(builder.ackExplicit).toHaveBeenCalled();
  });
});
