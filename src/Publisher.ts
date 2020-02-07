import {Message} from "./Message";
import {SQS} from 'aws-sdk';
import {MessageConverter, RawBatchMessage} from "./MessageConverter";
import {Queue} from "./Queue";
import * as is from 'predicates';
import * as debugModule from "debug";
import {debugFn} from './debugFn';

const assertGroupId = is.assert(
    is.prop('groupId', is.all(is.string, is.notBlank)),
    'Messages published to fifo queue require "groupId"'
);

const assertGroupIdNotDefined = is.assert(
    is.prop('groupId', is.undefined),
    'Messages published to standard queue cannot have "groupId" defined'
);

const assertDeduplicationId = is.assert(
    is.prop('deduplicationId', is.all(is.string, is.notBlank)),
    'Messages published to fifo queue without content deduplication enabled required "deduplicationId"'
);

const assertDeduplicationIdNotDefined = is.assert(
    is.prop('messageDeduplicationId', is.undefined),
    'Messages publish to standard queue cannot have "messageDeduplicationId" defined'
);

export class Publisher<TMessage extends Message<any, any>> {
    private debug: debugModule.IDebugger;

    constructor(private sqs: SQS, private messageConverter: MessageConverter, private queue: Queue.Info) {
        this.debug = debugFn('publisher:' + this.queue.name);
    }

    private validateMessage(message: Message.Input) {
        if (this.queue.isFifo) {
            assertGroupId(message);

            if (!this.queue.attributes.isContentBasedDeduplication) {
                assertDeduplicationId(message);
            }
        } else {
            assertGroupIdNotDefined(message);
            assertDeduplicationIdNotDefined(message);
        }
    }

    protected convertMessage(input: Message.Input<TMessage['body']> | Message.BatchInput<TMessage['body']>) {
        return this.messageConverter.toRawMessage(input);
    }

    publish(input: Message.Input<TMessage['body']>) {
        this.validateMessage(input);
        this.debug('Publishing message');
        return this.sqs.sendMessage(
            {
                ...this.convertMessage(input),
                QueueUrl: this.queue.url
            }
        ).promise()
    }

    async publishMany(messages: Array<Message.Input<TMessage['body']> | Message.BatchInput<TMessage['body']>>) {
        const result: SQS.SendMessageBatchResult = {
            Failed: [],
            Successful: []
        };

        messages.forEach(this.validateMessage, this);
        for (let i = 0; i < messages.length; i += 10) {
            const group = messages.slice(i, 10);
            this.debug(`Publishing messages in batch: amount - ${group.length}`);
            const groupResult = await this.sqs.sendMessageBatch({
                QueueUrl: this.queue.url,
                Entries: group.map(
                    message => {
                        return this.convertMessage(
                            Message.isBatchInput(message) ? message : Message.Input.toBatchInput(message)
                        ) as RawBatchMessage;
                    }
                )
            }, undefined).promise();
            result.Successful.push(...groupResult.Successful);
            result.Failed.push(...groupResult.Failed)
        }
        return result;
    }
}