import {Consumer} from '../src/Consumer';
import * as AWS from 'aws-sdk';
import * as sinon from 'sinon';
import * as faker from 'faker';
import {assert} from 'chai';

describe('Consumer', () => {
    let sqs: AWS.SQS;

    const QUEUE_NAME = faker.lorem.word();
    const QUEUE_NAME_FIFO = faker.lorem.word() + '.fifo';

    beforeEach(() => {
        sqs = sinon.createStubInstance(AWS.SQS);
    });

    describe('handling options', () => {
        it('converts to fifo if isFifo = true but queue name has no .fifo suffix', () => {
            const consumer = new Consumer(sqs, {
                isFifo: true,
                queueName: QUEUE_NAME
            });

            assert.strictEqual(consumer.queueName, QUEUE_NAME + '.fifo');
            assert.isTrue(consumer.isFifo);
        });

        it('sets isFifo to true if queueName contains .fifo suffix', () => {
            const consumer = new Consumer(sqs, {
                queueName: QUEUE_NAME_FIFO
            });
            assert.strictEqual(consumer.queueName, QUEUE_NAME_FIFO);
            assert.isTrue(consumer.isFifo);
        });

        it('minMessages cannot be greater than maxMessages', () => {
            assert.throws(() => {
                new Consumer(sqs, {
                    queueName: QUEUE_NAME,
                    minMessages: 5,
                    maxMessages: 4
                });
            }, Error, /cannot be higher than maxMessages/);
        });

        it('minMessages cannot be lower or equal 0', () => {
            assert.throws(() => {
                new Consumer(sqs, {
                    queueName: QUEUE_NAME,
                    minMessages: 0
                });
            }, Error, /must be greater than 0/);

            assert.throws(() => {
                new Consumer(sqs, {
                    queueName: QUEUE_NAME,
                    minMessages: -10
                });
            }, Error, /must be greater than 0/);
        });
    });

});