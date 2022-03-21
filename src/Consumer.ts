import * as AWS from 'aws-sdk';
import {EventEmitter} from 'events';
import {Message} from "./Message";
import {ResultContext, ResultHandler} from "./ResultContext";
import {debugFn} from './debugFn';
import * as debugModule from 'debug';
import {Queue} from "./Queue";
import {MessageConverter} from "./MessageConverter";
import {ulidPrefixedFactory} from "./requestAttemptGenerator";

export type ConsumerFunction<TMessage extends Message<any, any>> = (message: TMessage) => any;

export class Consumer<TMessage extends Message<any, any>> extends EventEmitter {

	public ongoingConsumptions = 0;
	public isRunning = false;

	private consumerToGroup: Map<string, Consumer.Definition<TMessage>> = new Map();
	private defaultConsumer!: Consumer.Definition<TMessage>;
	private receiveRequest?: AWS.Request<AWS.SQS.Types.ReceiveMessageResult, AWS.AWSError>;
	private poolScheduled = false;

	private debug: debugModule.IDebugger;

	private options: Consumer.Options;

	static defaultOptions: Consumer.Options = {
		maxMessages: 10,
		minMessages: 5,
		requestAttemptGenerator: ulidPrefixedFactory()
	};

	private requestAttemptId?: string;

	static defaultResultHandler: ResultHandler<any> = async (context: ResultContext<any>, error: any) => {
		if (!error) {
			await context.ack();
			return;
		}
		await context.reject();
	};

	constructor(private sqs: AWS.SQS,
				private messageConverter: MessageConverter,
				readonly queue: Queue.Info,
				options?: Partial<Consumer.Options>) {
		super();
		this.options = {
			...Consumer.defaultOptions,
			...options
		};
		this.assertOptionsCorrectness();

		this.debug = debugFn('consumer:' + this.queue.name);

		for (const event of ['rejected', 'consumed', 'retried']) {
			this.on(event, message => {
				this.debug(`Message - ${event} - ${message.sequenceNumber}`);
				this.decrementCounter();
				this.schedulePool();
			})
		}
	}

	get isStopped() {
		return !this.isRunning;
	}

	private decrementCounter() {
		this.ongoingConsumptions--;
		if (this.ongoingConsumptions === 0) {
			this.emit('all-consumed');
		}
	}

	private assertOptionsCorrectness() {
		if (this.options.minMessages > this.options.maxMessages) {
			throw new Error(`minMessages (${this.options.minMessages}) cannot be higher than maxMessages (${this.options.maxMessages})`);
		}

		if (this.options.minMessages <= 0) {
			throw new Error('minMessages must be greater than 0');
		}
	}

	/**
	 * Sets default consumer function that gets called when new message appears and there is no other consumer assigned to message group.
	 */
	onMessage(consumerFunction: ConsumerFunction<TMessage>, resultHandler?: ResultHandler<TMessage>): this {
		this.defaultConsumer = {
			consumerFunction,
			resultHandler: resultHandler || Consumer.defaultResultHandler
		};
		return this;
	}

	/**
	 * Sets consumer function for given message group.
	 */
	onGroupMessage(groupName: string, consumerFunction: ConsumerFunction<TMessage>, resultHandler?: ResultHandler<TMessage>): this {
		if (this.queue.isStandard) {
			throw new Error('Message groups are not supported by standard queues');
		}

		this.consumerToGroup.set(groupName, {
			consumerFunction,
			resultHandler: resultHandler || Consumer.defaultResultHandler
		});
		return this;
	}

	start() {
		if (this.isRunning) {
			throw new Error('Already running');
		}
		this.isRunning = true;
		if (!this.defaultConsumer) {
			throw new Error(`No default consumer for queue "${this.queue.name}"`);
		}
		this.debug('Starting consumption');
		this.pool();
	}

	private pool() {
		if (!this.requestAttemptId) {
			this.requestAttemptId = this.options.requestAttemptGenerator();
		}

		let messagesToFetch = this.options.maxMessages - this.ongoingConsumptions;
		if (messagesToFetch > 10) {
			messagesToFetch = 10;
		}
		this.debug(`Starting messages pooling (Amount of message ${messagesToFetch}). Attempt id: ${this.requestAttemptId}`);
		this.receiveRequest = this.sqs.receiveMessage({
			QueueUrl: this.queue.url,
			AttributeNames: ['All'],
			MessageAttributeNames: ['.*'],
			MaxNumberOfMessages: messagesToFetch,
			WaitTimeSeconds: 20,
			ReceiveRequestAttemptId: this.requestAttemptId
		}, (err: AWS.AWSError, result: AWS.SQS.Types.ReceiveMessageResult) => {
			this.receiveRequest = undefined;
			this.poolScheduled = false;

			if (err) {
				if (err.name === 'RequestAbortedError') {
					this.debug('Request aborted');
					this.schedulePool();
					return;
				}

				this.debug('Failed to fetch messages: ' + err.message);
				this.emit('error', err);
				this.schedulePool();
				return;
			}

			this.debug(`Messages found: ${(result.Messages && result.Messages.length) || 0}`);

			this.requestAttemptId = undefined;
			if (result.Messages && result.Messages.length) {
				result.Messages
					.map((m: AWS.SQS.Message) => {
						return this.messageConverter.fromRawMessage(m, this.queue) as TMessage;
					})
					.forEach(this.consumeMessage, this);
			}
			this.schedulePool();
		});
	}

	private schedulePool() {
		if (!this.isRunning) {
			return;
		}

		if (this.poolScheduled) {
			return;
		}
		// to prevent stack overflow
		setImmediate(() => {
			if (this.poolScheduled || !this.isRunning) {
				return;
			}

			if (this.ongoingConsumptions <= (this.options.maxMessages - this.options.minMessages)) {
				this.poolScheduled = true;
				this.pool();
			}
		});
	}

	private async consumeMessage(message: TMessage) {
		this.ongoingConsumptions++;

		this.debug(`Consuming message: ${message.sequenceNumber}`);
		if (message.groupId && this.consumerToGroup.has(message.groupId)) {
			await this.consumeMessageWithConsumer(
				message,
				this.consumerToGroup.get(message.groupId) as Consumer.Definition<TMessage>
			);
			return;
		}
		await this.consumeMessageWithConsumer(message, this.defaultConsumer);
	}

	private async consumeMessageWithConsumer(message: TMessage, consumerDefinition: Consumer.Definition<TMessage>) {
		const resultContext = new ResultContext(this.sqs, this, message);
		try {
			const result = await consumerDefinition.consumerFunction(message);
			await consumerDefinition.resultHandler(resultContext, undefined, result);
		} catch (e) {
			await consumerDefinition.resultHandler(resultContext, e);
		}
	}

	stop() {
		if (!this.isRunning) {
			throw new Error('Consumer not running');
		}

		this.debug('Stopping consumption of queue: ' + this.queue.name);
		if (this.receiveRequest) {
			this.receiveRequest.abort();
			this.receiveRequest = undefined;
		}

		this.isRunning = false;
	}
}

export namespace Consumer {

	export interface Definition<TMessage extends Message<any, any>> {
		consumerFunction: ConsumerFunction<TMessage>;
		resultHandler: ResultHandler<TMessage>;
	}

	export interface Options {
		/**
		 * Maximum amount of messages to receive within a single ReceiveMessage request
		 */
		maxMessages: number,

		/**
		 * Amount of messages needed to be consumed before starting pooling for another messages from a queue
		 */
		minMessages: number,

		/**
		 * A function that returns unique request prefix id
		 */
		requestAttemptGenerator: () => string
	}
}
