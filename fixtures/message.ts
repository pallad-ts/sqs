import {Message} from "../src/Message";
import {queue} from "./queue";

const bufferTest = Buffer.from('test', 'utf8');
const attributes = {
    raw: {
        some: {
            DataType: 'String',
            StringValue: 'string'
        },
        any: {
            DataType: 'Number',
            StringValue: '100'
        },
        bin: {
            DataType: 'Binary',
            BinaryValue: bufferTest
        }
    },
    parsed: {
        some: 'string',
        any: 100,
        bin: bufferTest
    }
};

const rawMessage = {
    ReceiptHandle: 'Some handle',
    MessageAttributes: attributes.raw,
    MD5OfBody: 'md5',
    Body: 'some-body',
    Attributes: {
        SenderId: '883215038568',
        ApproximateFirstReceiveTimestamp: '1559161428453',
        ApproximateReceiveCount: '1',
        SentTimestamp: '1559161405140',
        SequenceNumber: '18845889393425391616'
    }
};

export const message = {
    regular: new Message(
        rawMessage,
        queue.standard,
        'some-body',
        attributes.parsed
    ),
    noAttributes: new Message(
        {
            ...rawMessage,
            MessageAttributes: undefined
        },
        queue.standard,
        'some-body',
        undefined
    ),
    fifo: new Message(
        {
            ...rawMessage,
            Attributes: {
                ...rawMessage.Attributes,
                MessageGroupId: 'groupid'
            }
        },
        queue.fifo,
        'some-body',
        attributes.parsed
    ),
    fifoDedup: new Message(
        {
            ...rawMessage,
            Attributes: {
                ...rawMessage.Attributes,
                MessageGroupId: 'groupid',
                MessageDeduplicationId: 'dedupid'
            }
        },
        queue.fifoContentDeduplication,
        'some-body',
        attributes.parsed
    )
};

