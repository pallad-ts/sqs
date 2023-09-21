import {Queue} from "@src/Queue";

export const queue = {
	standard: new Queue.Info(
		'standard',
		'http://standard', {
			isFifo: false,
			retentionPeriod: 0,
			maxMessageSize: 100,
			visibilityTimeout: 100,
			receiveMessageWaitTime: 100,
			isContentBasedDeduplication: false,
			delay: 0,
			arn: 'arnstandard',
			redrivePolicy: undefined,
			redriveAllowPolicy: undefined
		}
	),
	fifo: new Queue.Info(
		'fifo.fifo',
		'http://fifo', {
			isFifo: true,
			retentionPeriod: 0,
			maxMessageSize: 100,
			visibilityTimeout: 100,
			receiveMessageWaitTime: 100,
			isContentBasedDeduplication: false,
			delay: 0,
			arn: 'arnfifo',
			redrivePolicy: undefined,
			redriveAllowPolicy: undefined
		}
	),
	fifoContentDeduplication: new Queue.Info(
		'fifo_dedup.fifo',
		'http://fifo_dedup', {
			isFifo: true,
			retentionPeriod: 0,
			maxMessageSize: 100,
			visibilityTimeout: 100,
			receiveMessageWaitTime: 100,
			isContentBasedDeduplication: true,
			delay: 0,
			arn: 'arnfifodedup',
			redrivePolicy: undefined,
			redriveAllowPolicy: undefined
		}
	)
};
