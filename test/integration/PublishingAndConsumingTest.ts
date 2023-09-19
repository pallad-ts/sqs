import {manager} from './config';
import {Publisher} from "@src/Publisher";
import * as fixtures from '../fixtures';
import {Message} from "@src/Message";
import {create} from "@pallad/id";
import {ResultContext} from "@src/ResultContext";

function createConsumerFunc() {
	let resolve: ((data: any) => void);
	const promise = new Promise<Message>(r => {
		resolve = r;
	});
	return {
		// consumer: resolve as unknown as (message: Message) => void,
		consumer(message: Message) {
			(resolve as any)(message);
		},
		promise
	};
}

describe('Publishing', () => {
	jest.setTimeout(30000);

	describe('standard queue', () => {
		const QUEUE = `alpha_publishing_standard`;
		const MESSAGE = fixtures.message.regular;

		let publisher: Publisher<any>;

		beforeAll(async () => {
			await manager.queueManager.assert(QUEUE);
			publisher = await manager.createPublisher(QUEUE);
		});

		afterEach(() => {
			manager.stopAllConsumers();
		});

		it('publishing', async () => {
			const result = await publisher.publish(MESSAGE.toInput());
			expect(result)
				.toMatchObject({
					MD5OfMessageBody: expect.any(String),
					MD5OfMessageAttributes: expect.any(String),
					MessageId: expect.any(String)
				});

			const consumerFunc = createConsumerFunc();
			await manager.consume(QUEUE, consumerFunc.consumer);
			const message = await consumerFunc.promise;

			expect(message)
				.toMatchObject({
					body: MESSAGE.body,
					attributes: MESSAGE.attributes
				});
		});

		it('consuming per message group is not supported by standard queue consumer', async () => {
			// tslint:disable-next-line:no-empty
			const consumer = await manager.consume(QUEUE, () => {
			});

			expect(() => {
				// tslint:disable-next-line:no-empty
				consumer.onGroupMessage('some-group', () => {

				})
			})
				.toThrowError(/not supported by standard queues/);
		});
	});

	describe('fifo queue without content deduplication', () => {
		const QUEUE = `alpha_publishing_fifo_without_dedup.fifo`;
		const MESSAGE = fixtures.message.fifoDedup;

		let publisher: Publisher<any>;

		beforeAll(async () => {
			await manager.queueManager.assert(QUEUE);
			publisher = await manager.createPublisher(QUEUE);
		});

		afterEach(async () => {
			await manager.stopAllConsumersAndWaitToFinish();
		});

		it('publishing', async () => {
			const result = await publisher.publish(MESSAGE.toInput());
			expect(result)
				.toMatchObject({
					MD5OfMessageBody: expect.any(String),
					MD5OfMessageAttributes: expect.any(String),
					MessageId: expect.any(String)
				});

			const consumerFunc = createConsumerFunc();
			await manager.consume(QUEUE, consumerFunc.consumer);
			const message = await consumerFunc.promise;

			expect(message)
				.toMatchObject({
					body: MESSAGE.body,
					attributes: MESSAGE.attributes,
					deduplicationId: MESSAGE.deduplicationId,
					groupId: MESSAGE.groupId
				});
		});

		it('consuming by group id', async () => {
			const GROUP_1 = 'group1';
			const GROUP_2 = 'group2';

			const MESSAGE_INPUT_1 = {
				...MESSAGE.toInput(),
				groupId: GROUP_1,
				deduplicationId: create()
			};
			const MESSAGE_INPUT_2: Message.Input = {
				...MESSAGE.toInput(),
				groupId: GROUP_2,
				deduplicationId: create()
			};

			const consumerFunc1 = createConsumerFunc();
			const consumerFunc2 = createConsumerFunc();

			const consumer = await manager.consume(QUEUE, consumerFunc1.consumer, {autoStart: false});
			consumer.onGroupMessage(GROUP_2, consumerFunc2.consumer);

			consumer.start();

			await publisher.publishMany([
				MESSAGE_INPUT_1,
				MESSAGE_INPUT_2
			]);

			const message1 = await consumerFunc1.promise;
			const message2 = await consumerFunc2.promise;

			expect(message1)
				.toMatchObject({
					deduplicationId: MESSAGE_INPUT_1.deduplicationId,
					groupId: GROUP_1
				});

			expect(message2)
				.toMatchObject({
					deduplicationId: MESSAGE_INPUT_2.deduplicationId,
					groupId: GROUP_2
				});
		}, 20000);
	});

	describe('consuming lot of messages at the same time', () => {
		const QUEUE = `alpha_publishing_many_regular`;
		const MESSAGE = fixtures.message.regular;

		let publisher: Publisher<any>;
		beforeAll(async () => {
			await manager.queueManager.assert(QUEUE);
			publisher = await manager.createPublisher(QUEUE);
		});

		it('40', async () => {
			const inputs = Array(120).fill(MESSAGE.toInput());

			let isReady;
			const promise = new Promise(resolve => isReady = resolve);
			await publisher.publishMany(inputs);
			const contexts: Array<ResultContext<any>> = [];
			const consumer = await manager.consume(QUEUE, message => {
				return true;
			}, {
				maxMessages: 40,
				minMessages: 5,
				autoStart: false,
				resultHandler(context) {
					contexts.push(context);
					if (contexts.length >= 40) {
						isReady();
					}
					return Promise.resolve(undefined);
				}
			});
			consumer.start();
			await promise;

			consumer.stop();
			let i = 0;
			for (const context of contexts) {
				await context.ack()
			}
			await manager.stopAllConsumersAndWaitToFinish();
		}, 60000);
	});
});
