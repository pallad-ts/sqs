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
});
