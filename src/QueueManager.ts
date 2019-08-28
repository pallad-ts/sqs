import {SQS, AWSError} from "aws-sdk";
import {Queue} from "./Queue";
import * as is from 'predicates';

const assertQueueName = is.assert(
    is.all(
        is.matches(/^[a-z0-9\-_]+(\.fifo)?$/i),
        (x: string) => x.length < 80,
    ),
    'Queue name cannot be blank, must contains only alphanumeric characters, hyphens and underscores and has maximum length of 80 characters'
);

// TODO add "Policy" and "RedrivePolicy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds"
function toRawAttributes(attributes: Queue.Attributes.Input): SQS.QueueAttributeMap {
    const result: SQS.QueueAttributeMap = {};

    if ('delay' in attributes) {
        result.DelaySeconds = String(attributes.delay);
    }

    if ('maxMessageSize' in attributes) {
        result.MaximumMessageSize = String(attributes.maxMessageSize);
    }

    if ('retentionPeriod' in attributes) {
        result.MessageRetentionPeriod = String(attributes.retentionPeriod);
    }

    if ('receiveMessageWaitTime' in attributes) {
        result.ReceiveMessageWaitTimeSeconds = String(attributes.receiveMessageWaitTime);
    }

    if ('visibilityTimeout' in attributes) {
        result.VisibilityTimeout = String(attributes.visibilityTimeout);
    }

    if (attributes.isFifo) {
        result.FifoQueue = 'true';
    }

    if ('isContentBasedDeduplication' in attributes) {
        result.ContentBasedDeduplication = attributes.isContentBasedDeduplication ? 'true' : 'false';
    }
    return result;
}

function fromRawToAttributes(attributes: SQS.QueueAttributeMap): Queue.Attributes {
    return {
        delay: parseFloat(attributes.DelaySeconds),
        isContentBasedDeduplication: attributes.ContentBasedDeduplication === 'true',
        isFifo: attributes.FifoQueue === 'true',
        maxMessageSize: parseFloat(attributes.MaximumMessageSize),
        receiveMessageWaitTime: parseFloat(attributes.ReceiveMessageWaitTimeSeconds),
        retentionPeriod: parseFloat(attributes.MessageRetentionPeriod),
        visibilityTimeout: parseFloat(attributes.VisibilityTimeout),
        arn: attributes.QueueArn
    };
}

export class QueueManager {
    private cachedQueueInfo: Map<string, Queue.Info> = new Map();
    private cachedPromises: Map<string, Promise<Queue.Info>> = new Map();

    constructor(readonly sqs: SQS) {

    }

    async getInfo(name: string, accountId?: string): Promise<Queue.Info> {
        assertQueueName(name);
        const key = `name:${name};accountId:${accountId ? accountId : ''}`;

        if (this.cachedQueueInfo.has(key)) {
            return this.cachedQueueInfo.get(key) as Queue.Info;
        }

        if (this.cachedPromises.has(key)) {
            return this.cachedPromises.get(key) as Promise<Queue.Info>;
        }

        const promise = this.getInfoInternal(name, accountId);
        this.cachedPromises.set(key, promise);
        promise.catch(e => this.cachedPromises.delete(key));

        const result = await promise;
        this.cachedPromises.delete(key);
        this.cachedQueueInfo.set(key, result);

        return result;
    }

    async create(name: string, options?: Queue.Attributes.Input): Promise<string> {
        assertQueueName(name);
        const result = await this.sqs.createQueue({
            QueueName: name,
            Attributes: toRawAttributes(options || {
                isFifo: Queue.isFifo(name)
            })
        }).promise();

        return result.QueueUrl as string;
    }

    /**
     * Makes sure that queue with given name exists. Does not update attributes even when differ.
     */
    async assert(name: string, options?: Queue.Attributes.Input): Promise<Queue.Info> {
        try {
            return await this.getInfo(name);
        } catch (e) {
            if (e.code === 'AWS.SimpleQueueService.NonExistentQueue') {
                await this.create(name, options);
                return this.getInfo(name);
            }
            throw e;
        }
    }

    private async getQueueUrl(name: string, accountId?: string): Promise<string> {
        const result = await this.sqs.getQueueUrl({
            QueueName: name,
            QueueOwnerAWSAccountId: accountId
        }).promise();

        if (!result.QueueUrl) {
            throw new Error(`Something went wrong with getting queue URL: ${name}, accountId: ${accountId}`);
        }
        return result.QueueUrl;
    }

    private async getInfoInternal(name: string, accountId?: string) {
        const url = await this.getQueueUrl(name, accountId);

        const attributes = [
            "DelaySeconds",
            "RedrivePolicy",
            "KmsDataKeyReusePeriodSeconds",
            "KmsMasterKeyId",
            "MaximumMessageSize",
            "MessageRetentionPeriod",
            "ReceiveMessageWaitTimeSeconds",
            "VisibilityTimeout",
            "QueueArn"
        ];

        if (Queue.isFifo(name)) {
            attributes.push("FifoQueue", "ContentBasedDeduplication");
        }

        const resultAttributes = await this.sqs.getQueueAttributes({
            QueueUrl: url,
            AttributeNames: attributes
        }).promise();

        if (!resultAttributes.Attributes) {
            throw new Error(`Something went wrong with getting queue attributes: ${name}, accountId: ${accountId}`);
        }

        return new Queue.Info(
            name,
            url,
            fromRawToAttributes(resultAttributes.Attributes)
        );
    }

    async delete(name: string, accountId?: string) {
        const url = await this.getQueueUrl(name, accountId);
        await this.sqs.deleteQueue({
            QueueUrl: url
        }).promise();
    }
}