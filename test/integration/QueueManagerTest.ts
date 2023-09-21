import {sqs} from './config';
import {DeleteQueueCommand} from "@aws-sdk/client-sqs";
import {QueueManager} from "@src/QueueManager";
import {v4} from 'uuid';

describe('QueueManager', () => {
	let manager: QueueManager;

	beforeEach(() => {
		manager = new QueueManager(sqs);
	});

	describe('asserting queue', () => {
		const QUEUE_NAME = `pallad-sqs-${v4()}`;

		it('creates queue if does not exist', async () => {
			const queue = await manager.assert(QUEUE_NAME);

			expect(queue)
				.toBeDefined();
		});

		afterAll(() => {
			return manager.delete(QUEUE_NAME);
		});
	});

	it('redrive attributes', async () => {
		const redriveStorageQueue = await manager.assert('pallad-sqs-redrive-storage', {
			redriveAllowPolicy: "allowAll"
		});

		const queueToRedrive = await manager.assert('pallad-sqs-redrive-source', {
			redrivePolicy: {
				deadLetterQueueArn: redriveStorageQueue.attributes.arn,
			}
		});

		expect(redriveStorageQueue.attributes.redriveAllowPolicy)
			.toBe('allowAll');

		expect(queueToRedrive.attributes.redrivePolicy)
			.toEqual({
				deadLetterQueueArn: redriveStorageQueue.attributes.arn,
				maxReceiveCount: 10
			});
	})
});
