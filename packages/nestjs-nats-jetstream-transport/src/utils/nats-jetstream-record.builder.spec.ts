/** Unit (no server): record-builder outcomes a JetStream publish would use. */
import { NatsJetStreamRecord, NatsJetStreamRecordBuilder } from './nats-jetstream-record.builder';

describe('NatsJetStreamRecordBuilder', () => {
  it('builds a record carrying payload and default empty options', () => {
    const rec = new NatsJetStreamRecordBuilder({ id: 1 }).build();
    expect(rec).toBeInstanceOf(NatsJetStreamRecord);
    expect(rec.payload).toEqual({ id: 1 });
    expect(rec.options).toEqual({});
  });

  it('setPayload replaces payload; constructor payload is kept otherwise', () => {
    const b = new NatsJetStreamRecordBuilder<string>('first');
    b.setPayload('second');
    expect(b.build().payload).toBe('second');
  });

  it('setMsgId / getMsgId round-trip and land in publish options', () => {
    const b = new NatsJetStreamRecordBuilder({ id: 2 });
    b.setMsgId('msg-42');
    expect(b.getMsgId()).toBe('msg-42');
    expect(b.build().options.msgID).toBe('msg-42');
  });

  it('appendHeader accumulates headers readable via the nats headers API', () => {
    const b = new NatsJetStreamRecordBuilder({ id: 3 });
    b.appendHeader('x-trace', 't-1');
    b.appendHeader('x-trace', 't-2');
    b.appendHeader('x-other', 'o');
    const headers = b.build().options.headers;
    expect(headers.get('x-trace')).toBe('t-1');
    expect(headers.values('x-trace')).toEqual(['t-1', 't-2']);
    expect(headers.get('x-other')).toBe('o');
  });

  it('setExpectations lands in publish options.expect', () => {
    const b = new NatsJetStreamRecordBuilder({ id: 4 });
    b.setExpectations({ streamName: 'mystream', lastSequence: 9, lastMsgID: 'prev' });
    expect(b.build().options.expect).toEqual({
      streamName: 'mystream',
      lastSequence: 9,
      lastMsgID: 'prev',
    });
  });
});
