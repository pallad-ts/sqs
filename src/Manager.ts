import {Consumer, ConsumerFunction} from "./Consumer";
import {ResultHandler} from "./ResultContext";
import {QueueManager} from "./QueueManager";
import {Publisher} from "./Publisher";
import {MessageConverter} from "./MessageConverter";
import {Message} from "./Message";
import {globalAgent, Agent} from 'https';

export class Manager {
	private consumers: Set<Consumer<any>> = new Set();

	constructor(readonly queueManager: QueueManager,
				readonly messageConverter: MessageConverter) {

	}

	async createPublisher(queueName: string, accountId?: string) {
		const queueInfo = await this.queueManager.getInfo(queueName, accountId)
		if (queueInfo) {
			return new Publisher(this.queueManager.sqsClient, this.messageConverter, queueInfo);
		}
		throw new Error(`Cannot create published for queue that does not exist: ${queueName}`);
	}

	async consume<TMessage extends Message<any, any>>(queueName: string,
													  func: ConsumerFunction<TMessage>,
													  options: Manager.ConsumerOptions<TMessage> = {}): Promise<Consumer<TMessage>> {
		const queueInfo = await this.queueManager.getInfo(queueName);
		if (!queueInfo) {
			throw new Error(`Cannot create consumer for queue that does not exist: ${queueName}`);
		}
		const {resultHandler, autoStart = true, ...consumerOptions} = options;
		const consumer = new Consumer<TMessage>(this.queueManager.sqsClient, this.messageConverter, queueInfo, consumerOptions);
		consumer.onMessage(func, resultHandler);
		this.consumers.add(consumer);
		if (autoStart) {
			consumer.start();
		}
		return consumer;
	}

	/**
	 * Stops all consumers
	 */
	stopAllConsumers() {
		for (const consumer of this.consumers) {
			if (consumer.isRunning) {
				consumer.stop();
			}
		}
	}

	/**
	 * Stops all consumers and wait for them to finish current message consumptions
	 */
	stopAllConsumersAndWaitToFinish(): Promise<void> {
		return new Promise(resolve => {
			this.stopAllConsumers();

			const getTotalOngoingConsumptions = () => {
				return Array.from(this.consumers.values())
					.reduce((total, c) => {
						return total + c.ongoingConsumptions;
					}, 0);
			};

			if (getTotalOngoingConsumptions() === 0) {
				resolve();
			} else {
				for (const consumer of this.consumers) {
					consumer.once('all-consumed', () => {
						if (getTotalOngoingConsumptions() === 0) {
							resolve();
						}
					});
				}
			}
		});
	}
}

export namespace Manager {
	export interface ConsumerOptions<TMessage extends Message<any, any>> extends Partial<Consumer.Options> {
		resultHandler?: ResultHandler<TMessage>,
		autoStart?: boolean
	}
}
