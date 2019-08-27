import {manager} from './config';
import {Publisher} from "../src/Publisher";
import * as fixtures from '../fixtures';
import {String} from "aws-sdk/clients/sqs";
import {Message} from "../src/Message";
import {ulid} from "ulid";

function createConsumerFunc() {
    let resolve: (data: any) => void;
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
    describe('standard queue', () => {
        const QUEUE = 'alpha_publishing_standard';
        const MESSAGE = fixtures.message.regular;

        let publisher: Publisher;

        beforeAll(async () => {
            await manager.queueManager.assert(QUEUE);
            publisher = await manager.createPublisher(QUEUE);

        });

        afterEach(() => {
            return manager.stopAllConsumers();
        });

        afterAll(async () => {
            await manager.queueManager.delete(QUEUE);
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
            const consumer = await manager.consume(QUEUE, () => {

            });

            expect(() => {
                consumer.onGroupMessage('some-group', () => {

                })
            })
                .toThrowError(/not supported by standard queues/);
        });
    });

    describe('fifo queue without content deduplication', () => {
        const QUEUE = 'alpha_publishing_fifo_without_dedup.fifo';
        const MESSAGE = fixtures.message.fifoDedup;

        let publisher: Publisher;

        beforeAll(async () => {
            await manager.queueManager.assert(QUEUE);
            publisher = await manager.createPublisher(QUEUE);
        });

        afterEach(async () => {
            await manager.stopAllConsumersAndWaitToFinish();
        });

        afterAll(async () => {
            await manager.queueManager.delete(QUEUE);
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
            const GROUP_1 = MESSAGE.groupId;
            const GROUP_2 = 'group2';

            const MESSAGE_INPUT_1 = {
                ...MESSAGE.toInput(),
                deduplicationId: ulid()
            };
            const MESSAGE_INPUT_2: Message.Input = {
                ...MESSAGE.toInput(),
                groupId: GROUP_2,
                deduplicationId: ulid()
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
});