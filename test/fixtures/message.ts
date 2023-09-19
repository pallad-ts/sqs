import {queue} from "./queue";
import {Message} from "@src/Message";

const typedArray = new Uint8Array(Buffer.from('test', 'utf8'));
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
			BinaryValue: typedArray
		}
	},
	parsed: {
		some: 'string',
		any: 100,
		bin: typedArray
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
		{}
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

