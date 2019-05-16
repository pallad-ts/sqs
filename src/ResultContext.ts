import {Message} from './Message';
import * as AWS from 'aws-sdk';
import {Consumer} from "./Consumer";
import {RetryAttributes, RetryTopologyEnchanted} from "./RetryTopology";
import {
    createDeduplicationIdForRetry, getAlphaAttributes, isFifoQueue,
    removeUnsupportedMessageAttributesValues
} from "./helpers";

/**
 * Type of function responsible for handling a consumer result.
 * Function MUST call one of ResultContext methods in order mark messages are rejected, acknowledge it or retry consumption
 */
export type ResultHandler = (context: ResultContext, error?: any, result?: any) => Promise<void>;

export class ResultContext {

    private retryTopology: RetryTopologyEnchanted;

    constructor(private sqs: AWS.SQS, private consumer: Consumer, private message: Message) {

    }

    setEnchantedRetryTopology(retryTopology: RetryTopologyEnchanted) {
        this.retryTopology = retryTopology;
    }

    async ack() {
        await this.sqs.deleteMessage({
            QueueUrl: this.message.queueURL,
            ReceiptHandle: this.message.sqsMessage.ReceiptHandle
        }).promise();

        this.consumer.emit('consumed', this.message);
    }

    async reject() {
        await this.sqs.changeMessageVisibility({
            QueueUrl: this.message.meta.queueURL,
            ReceiptHandle: this.message.raw.ReceiptHandle,
            VisibilityTimeout: 0
        }).promise();

        this.consumer.emit('rejected', this.message);
    }

    async retry() {
        if (!this.retryTopology) {
            throw new Error('You cannot retry message consumption without retry topology setup. Call "setupRetryTopology" first.')
        }

        const alphaAttributes = getAlphaAttributes(this.message);
        const newMessage: AWS.SQS.Types.SendMessageRequest = {
            QueueUrl: this.retryTopology.queueUrl,
            MessageBody: this.message.body,
            MessageAttributes: Object.assign({}, this.message.sqsMessage.MessageAttributes)
        };

        if (isFifoQueue(this.retryTopology.queueUrl)) {
            newMessage.MessageGroupId = 'messagesToRetry';
            newMessage.MessageDeduplicationId = createDeduplicationIdForRetry(
                this.message.messageDeduplicationId,
                alphaAttributes.retryAttempt + 1
            )
        }

        removeUnsupportedMessageAttributesValues(newMessage.MessageAttributes);
        newMessage.MessageAttributes[RetryAttributes.retryAttempt] = {
            StringValue: (alphaAttributes.retryAttempt + 1) + '',
            DataType: 'Number'
        };

        newMessage.MessageAttributes[RetryAttributes.queueUrl] = {
            StringValue: this.message.queueURL,
            DataType: 'String'
        };

        newMessage.MessageAttributes[RetryAttributes.messageGroup] = {
            StringValue: this.message.messageGroupId,
            DataType: 'String'
        };

        newMessage.MessageAttributes[RetryAttributes.date] = {
            StringValue: (Date.now() + this.retryTopology.retryDelay) + '',
            DataType: 'Number'
        };

        await this.sqs.sendMessage(newMessage).promise();
        await this.sqs.deleteMessage({
            QueueUrl: this.message.queueURL,
            ReceiptHandle: this.message.sqsMessage.ReceiptHandle
        }).promise();

        this.consumer.emit('retried', this.message);
    }
}