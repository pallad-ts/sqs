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
		const queueInfo = await this.queueManager.getInfo(queueName, accountId);
		return new Publisher(this.queueManager.sqs, this.messageConverter, queueInfo);
	}

	async consume<TMessage extends Message<any, any>>(queueName: string,
													  func: ConsumerFunction<TMessage>,
													  options: Manager.ConsumerOptions<TMessage> = {}): Promise<Consumer<TMessage>> {
		const queueInfo = await this.queueManager.getInfo(queueName);
		const {resultHandler, autoStart = true, ...consumerOptions} = options;
		const consumer = new Consumer<TMessage>(this.queueManager.sqs, this.messageConverter, queueInfo, consumerOptions);
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
							const agent = this.queueManager.sqs.config.httpOptions?.agent;
							if (agent && Object.is(agent, globalAgent)) {
								agent.destroy();
							}
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
