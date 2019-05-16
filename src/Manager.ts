import * as AWS from 'aws-sdk';
import {Consumer, ConsumerFunction, ConsumerOptions} from "./Consumer";
import {RetryAttributes, RetryTopology, RetryTopologyEnchanted} from "./RetryTopology";
import {Message} from "./Message";
import {ResultContext, ResultHandler} from "./ResultContext";
import {getAlphaAttributes, isFifoQueue, removeUnsupportedMessageAttributesValues} from "./helpers";


interface ManagerConsumerOptions extends ConsumerOptions {
    resultHandler?: ResultHandler,
    consumerFunction: ConsumerFunction
};

export class Manager {
    private consumers: Consumer[] = [];

    private retryTopologyEnchanted: RetryTopologyEnchanted;

    private retryConsumer: Consumer;

    constructor(private sqs: AWS.SQS) {

    }

    async consume(options: ManagerConsumerOptions): Promise<Consumer> {
        const {consumerFunction, resultHandler, ...otherOptions} = options;
        const consumer = new Consumer(this.sqs, options);
        consumer.onMessage(consumerFunction, resultHandler);

        if (this.retryTopologyEnchanted) {
            consumer.setEnchantedRetryTopology(this.retryTopologyEnchanted);
        }
        this.consumers.push(consumer);
        await consumer.start();
        return consumer;
    }

    /**
     * Stops all consumers
     */
    stopAllConsumers() {
        for (const consumer of this.consumers) {
            consumer.stop();
        }
    }

    /**
     * Stops all consumers and wait for them to finish current message consumptions
     */
    stopAllConsumersAndWaitToFinish(): Promise<void> {
        return new Promise((resolve) => {
            this.stopAllConsumers();

            const getTotalOngoingConsumptions = () => {
                return this.consumers.reduce((total, c) => {
                    return total + c.ongoingConsumptions;
                }, 0);
            };

            for (const consumer of this.consumers) {
                consumer.once('all-consumed', () => {
                    if (getTotalOngoingConsumptions() === 0) {
                        resolve();
                    }
                });
            }
        });
    }

    /**
     * Setups topology necessary for
     *
     * @param {RetryTopology} topology
     * @returns {Promise<void>}
     */
    async setupRetryTopology(topology: RetryTopology) {
        this.retryConsumer = new Consumer(this.sqs, {
            queueName: topology.queueName,
            minMessages: 1,
            maxMessages: 1
        });

        const retryResultHandler: ResultHandler = async (context: ResultContext, error: Error, result?: boolean) => {
            if (result === true) {
                await context.ack();
                return;
            }
            await context.reject();
        };

        const consumerFunction: ConsumerFunction = async (message: Message) => {
            const alphaAttributes = getAlphaAttributes(message);

            if (alphaAttributes.date > Date.now()) {
                this.retryConsumer.stop();
                setTimeout(this.retryConsumer.start.bind(this.retryConsumer), alphaAttributes.date - Date.now());
                return false;
            }

            const newMessage: AWS.SQS.Types.SendMessageRequest = {
                QueueUrl: alphaAttributes.targetQueueUrl,
                MessageBody: message.sqsMessage.Body,
                MessageAttributes: Object.assign({}, message.sqsMessage.MessageAttributes)
            };

            if (isFifoQueue(alphaAttributes.targetQueueUrl)) {
                newMessage.MessageGroupId = alphaAttributes.messageGroup;

                let newDeduplicationId = message.messageDeduplicationId || '';
                newDeduplicationId += '_retry' + alphaAttributes.retryAttempt;
                newMessage.MessageDeduplicationId = newDeduplicationId;
            }

            ALPHA_ATTRIBUTES_KEYS.forEach(k => {
                delete newMessage.MessageAttributes[k];
            });

            removeUnsupportedMessageAttributesValues(newMessage.MessageAttributes);

            await this.sqs.sendMessage(newMessage).promise();
            return true;
        };

        this.retryConsumer.onMessage(consumerFunction, retryResultHandler);
        await this.retryConsumer.start();
        this.retryTopologyEnchanted = {...topology, queueUrl: this.retryConsumer.queueUrl};

        for (const consumer of this.consumers) {
            consumer.setEnchantedRetryTopology(this.retryTopologyEnchanted);
        }
    }
}

const ALPHA_ATTRIBUTES_KEYS = [
    RetryAttributes.messageGroup,
    RetryAttributes.queueUrl,
    RetryAttributes.date
];
