import {Queue} from "./Queue";
import * as is from 'predicates';
import {CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient} from '@aws-sdk/client-sqs';

const assertQueueName = is.assert(
	is.all(
		is.matches(/^[a-z0-9\-_]+(\.fifo)?$/i),
		(x: string) => x.length < 80,
	),
	'Queue name cannot be blank, must contains only alphanumeric characters, hyphens and underscores and has maximum length of 80 characters'
);

// TODO add "Policy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds"
function toRawAttributes(attributes: Queue.Attributes.Input): Record<string, string> {
	const result: Record<string, string> = {};

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

	if (attributes.redrivePolicy) {
		result.RedrivePolicy = fromRedrivePolicyToRaw(attributes.redrivePolicy);
	}
	if (attributes.redriveAllowPolicy) {
		result.RedriveAllowPolicy = fromRedriveAllowPolicyToRaw(attributes.redriveAllowPolicy);
	}
	return result;
}

function fromRawToAttributes(attributes: Record<string, string>): Queue.Attributes {
	return {
		delay: parseFloat(attributes.DelaySeconds),
		isContentBasedDeduplication: attributes.ContentBasedDeduplication === 'true',
		isFifo: attributes.FifoQueue === 'true',
		maxMessageSize: parseFloat(attributes.MaximumMessageSize),
		receiveMessageWaitTime: parseFloat(attributes.ReceiveMessageWaitTimeSeconds),
		retentionPeriod: parseFloat(attributes.MessageRetentionPeriod),
		visibilityTimeout: parseFloat(attributes.VisibilityTimeout),
		arn: attributes.QueueArn,
		redriveAllowPolicy: attributes.RedriveAllowPolicy ? fromRawToRedriveAllowPolicy(attributes.RedriveAllowPolicy) : undefined,
		redrivePolicy: attributes.RedrivePolicy ? fromRawToRedrivePolicy(attributes.RedrivePolicy) : undefined
	};
}

function fromRedrivePolicyToRaw(policy: Queue.RedrivePolicy.Input) {
	return JSON.stringify({
		deadLetterTargetArn: policy.deadLetterQueueArn,
		maxReceiveCount: policy.maxReceiveCount ?? 10
	});
}

function fromRedriveAllowPolicyToRaw(policy: Queue.RedriveAllowPolicy) {
	if (policy === 'allowAll' || policy === 'denyAll') {
		return JSON.stringify({redrivePermission: policy});
	}
	return JSON.stringify({
		redrivePermission: 'byQueue',
		sourceQueueArns: policy.sourceQueueArns
	});
}

function fromRawToRedriveAllowPolicy(jsonString: string): Queue.RedriveAllowPolicy {
	const data = JSON.parse(jsonString);

	if (data.redrivePermission === 'allowAll' || data.redrivePermission === 'denyAll') {
		return data.redrivePermission;
	}

	return {sourceQueueArns: data.sourceQueueArns as string[]}
}

function fromRawToRedrivePolicy(jsonString: string): Queue.RedrivePolicy {
	const data = JSON.parse(jsonString);

	return {
		deadLetterQueueArn: data.deadLetterTargetArn,
		maxReceiveCount: parseInt(data.maxReceiveCount, 10)
	};
}

export class QueueManager {
	private cachedQueueInfoPromises: Map<string, Promise<Queue.Info | undefined>> = new Map();

	constructor(readonly sqsClient: SQSClient) {

	}

	async getInfo(name: string, accountId?: string): Promise<Queue.Info | undefined> {
		assertQueueName(name);
		const key = this.getQueueCacheKey(name, accountId);

		if (this.cachedQueueInfoPromises.has(key)) {
			return this.cachedQueueInfoPromises.get(key)!;
		}

		const promise = this.getInfoInternal(name, accountId);
		this.cachedQueueInfoPromises.set(key, promise);
		promise.then(result => {
			if (result === undefined) {
				this.cachedQueueInfoPromises.delete(key);
			}
		})
			.catch(() => {
				this.cachedQueueInfoPromises.delete(key);
			})
		return promise;
	}

	private getQueueCacheKey(name: string, accountId?: string) {
		return `name:${name};accountId:${accountId ? accountId : ''}`;
	}

	async create(name: string, options?: Queue.Attributes.Input): Promise<string> {
		assertQueueName(name);
		const result = await this.sqsClient.send(
			new CreateQueueCommand({
				QueueName: name,
				Attributes: toRawAttributes(options || {
					isFifo: Queue.isFifo(name)
				})
			})
		);

		this.clearCacheForQueue(name);
		return result.QueueUrl as string;
	}

	private clearCacheForQueue(name: string) {
		const cacheKey = this.getQueueCacheKey(name);
		this.cachedQueueInfoPromises.delete(cacheKey);
	}

	/**
	 * Makes sure that queue with given name exists. Does not update attributes even when differ.
	 */
	async assert(name: string, options?: Queue.Attributes.Input): Promise<Queue.Info> {
		const info = await this.getInfo(name);
		if (!info) {
			await this.create(name, options);
			const newInfo = await this.getInfo(name)
			return newInfo!;
		}
		return info;
	}

	private async getQueueUrl(name: string, accountId?: string): Promise<string | undefined> {
		try {
			const result = await this.sqsClient.send(new GetQueueUrlCommand({
				QueueName: name,
				QueueOwnerAWSAccountId: accountId
			}));

			return result.QueueUrl;
		} catch (e: any) {
			if (e.Error?.Code === NON_EXISTENT_QUEUE_ERROR_CODE || e.Code === NON_EXISTENT_QUEUE_ERROR_CODE) {
				return undefined;
			}

			throw e;
		}
	}

	private async getInfoInternal(name: string, accountId?: string) {
		const url = await this.getQueueUrl(name, accountId);

		if (!url) {
			return;
		}

		const attributes = [
			"DelaySeconds",
			"RedrivePolicy",
			"KmsDataKeyReusePeriodSeconds",
			"KmsMasterKeyId",
			"MaximumMessageSize",
			"MessageRetentionPeriod",
			"ReceiveMessageWaitTimeSeconds",
			"VisibilityTimeout",
			"QueueArn",
			"RedriveAllowPolicy"
		];

		if (Queue.isFifo(name)) {
			attributes.push("FifoQueue", "ContentBasedDeduplication");
		}

		const resultAttributes = await this.sqsClient.send(new GetQueueAttributesCommand({
			QueueUrl: url,
			AttributeNames: attributes
		}));

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
		await this.sqsClient.send(new DeleteQueueCommand({
			QueueUrl: url
		}));
	}
}


const NON_EXISTENT_QUEUE_ERROR_CODE = 'AWS.SimpleQueueService.NonExistentQueue';
