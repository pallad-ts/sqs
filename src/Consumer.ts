import * as AWS from 'aws-sdk';
import {EventEmitter} from 'events';
import {Message} from "./Message";
import {ResultContext, ResultHandler} from "./ResultContext";
import debug from './debug';
import * as debugModule from 'debug';
import {isFifoQueue} from "./helpers";
import {RetryTopologyEnchanted} from "./RetryTopology";

const randomId = require('random-id');

export interface ConsumerOptions {
    /**
     * Queue name to consume. Setting ".fifo" suffix automatically sets isFifo flag to true.
     */
    queueName: string;

    /**
     * Explicitly require queue to be a FIFO queue
     */
    isFifo?: boolean,

    /**
     * Maximum amount of messages to receive within a single ReceiveMessage request
     */
    maxMessages?: number,

    /**
     * Amount of messages needed to be consumed before starting pooling for another messages from a queue
     */
    minMessages?: number
}

export type ConsumerFunction = (message?: Message) => any;

type ConsumerDefinition = {
    consumerFunction: ConsumerFunction,
    resultHandler: ResultHandler
}

export class Consumer extends EventEmitter {

    public ongoingConsumptions = 0;
    public isRunning = false;
    public queueUrl: string;

    private consumerToGroup: Map<string, ConsumerDefinition> = new Map();
    private defaultConsumer: ConsumerDefinition;
    private receiveRequest: AWS.Request<AWS.SQS.Types.ReceiveMessageResult, AWS.AWSError>;
    private requestAttemptId: string;
    private requestAttemptPrefix: string;
    private poolScheduled = false;
    private enchantedRetryTopology: RetryTopologyEnchanted;

    private debug: debugModule.IDebugger;

    static defaultOptions: Partial<ConsumerOptions> = {
        isFifo: true,
        maxMessages: 10,
        minMessages: 5
    };

    static defaultResultHandler: ResultHandler = async (context: ResultContext, error: any) => {
        if (!error) {
            await context.ack();
            return;
        }
        await context.reject();
    };

    constructor(private sqs: AWS.SQS, private options: ConsumerOptions) {
        super();
        this.options = Object.assign({}, Consumer.defaultOptions, this.options);
        this.assertOptionsCorrectness();

        for (const event of ['rejected', 'consumed', 'retried']) {
            this.on(event, () => {
                this.decrementCounter();
                this.schedulePool();
            })
        }

        this.requestAttemptPrefix = randomId();
        this.debug = debug('consumer:' + this.options.queueName);
    }

    get queueName() {
        return this.options.queueName
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
        if (!this.options.queueName) {
            throw new Error('Queue name cannot be empty');
        }

        const hasFifoSuffix = isFifoQueue(this.options.queueName);

        if (!hasFifoSuffix && this.options.isFifo) {
            this.options.queueName += '.fifo';
        }

        if (!this.options.isFifo && hasFifoSuffix) {
            console.warn(`You have set queue name with ".fifo" suffix (${this.options.queueName}) but explicitly set "isFifo" flag to false. 
            In that case "isFifo" flag is changed to true. Fix it in order to remove this message.`);
            this.options.isFifo = true;
        }

        if (this.options.minMessages > this.options.maxMessages) {
            throw new Error(`minMessages (${this.options.minMessages}) cannot be higher than maxMessages (${this.options.maxMessages})`);
        }

        if (this.options.minMessages <= 0) {
            throw new Error('minMessages must be greater than 0');
        }
    }

    get isFifo() {
        return this.options.isFifo;
    }

    setEnchantedRetryTopology(retryTopology: RetryTopologyEnchanted) {
        this.enchantedRetryTopology = retryTopology;
    }

    /**
     * Sets default consumer function that gets called when new message appears and there is no other consumer assigned to message group.
     *
     * @param {ConsumerFunction} consumerFunction
     * @param {ResultHandler} resultHandler
     */
    onMessage(consumerFunction: ConsumerFunction, resultHandler?: ResultHandler): this {
        this.defaultConsumer = {
            consumerFunction,
            resultHandler: resultHandler || Consumer.defaultResultHandler
        };
        return this;
    }

    /**
     * Sets consumer function for given message group.
     */
    onGroupMessage(groupName: string, consumerFunction: ConsumerFunction, resultHandler?: ResultHandler): this {
        if (!this.isFifo) {
            throw new Error('Message groups are not supported by standard queues');
        }

        this.consumerToGroup.set(groupName, {
            consumerFunction,
            resultHandler: resultHandler || Consumer.defaultResultHandler
        });
        return this;
    }

    async start() {
        if (this.isRunning) {
            throw new Error('Already running');
        }

        this.isRunning = true;
        if (!this.queueUrl) {
            this.queueUrl = (await this.sqs.getQueueUrl({
                QueueName: this.options.queueName
            }).promise()).QueueUrl;
        }

        if (!this.defaultConsumer) {
            throw new Error(`No default consumer for queue "${this.options.queueName}"`);
        }

        this.pool();
    }

    private pool() {
        if (!this.requestAttemptId) {
            this.requestAttemptId = this.generateRequestAttemptId();
        }

        this.debug('Starting messages pool. Attempt id: ' + this.requestAttemptId);
        this.receiveRequest = this.sqs.receiveMessage({
            QueueUrl: this.queueUrl,
            AttributeNames: ['All'],
            MessageAttributeNames: ['.*'],
            MaxNumberOfMessages: this.options.maxMessages - this.ongoingConsumptions,
            WaitTimeSeconds: 20,
            ReceiveRequestAttemptId: this.requestAttemptId
        }, (err: AWS.AWSError, result: AWS.SQS.Types.ReceiveMessageResult) => {
            this.receiveRequest = undefined;
            this.poolScheduled = false;

            if (err) {
                this.debug('Failed to fetch messages: ' + err.message);
                this.emit('error', err);
                this.schedulePool();
                return;
            }

            this.debug('Messages found: ' + ((result.Messages && result.Messages.length) || 0));

            this.requestAttemptId = undefined;
            if (result.Messages && result.Messages.length) {
                result.Messages
                    .map((m: AWS.SQS.Message) => {
                        return new Message(m, this.options.queueName, this.queueUrl)
                    })
                    .forEach(this.consumeMessage, this);
            }

            this.schedulePool();
        });
    }

    private generateRequestAttemptId() {
        return this.requestAttemptPrefix + Date.now();
    }

    private schedulePool() {
        if (this.poolScheduled) {
            return;
        }
        // to prevent stack overflow
        setImmediate(() => {
            if (this.poolScheduled || !this.isRunning) {
                return;
            }

            if (this.ongoingConsumptions <= this.options.maxMessages - this.options.minMessages) {
                this.poolScheduled = true;
                this.pool();
            }
        });
    }

    private async consumeMessage(message: Message) {
        this.ongoingConsumptions++;

        if (message.messageGroupId && this.consumerToGroup.has(message.messageGroupId)) {
            await this.consumeMessageWithConsumer(
                message,
                <ConsumerDefinition>this.consumerToGroup.get(message.messageGroupId)
            );
            return;
        }

        await this.consumeMessageWithConsumer(message, this.defaultConsumer);
    }

    private async consumeMessageWithConsumer(message: Message, consumerDefinition: ConsumerDefinition) {
        const resultContext = new ResultContext(this.sqs, this, message);
        resultContext.setEnchantedRetryTopology(this.enchantedRetryTopology);
        try {
            const result = await consumerDefinition.consumerFunction(message);
            await consumerDefinition.resultHandler(resultContext, null, result);
        } catch (e) {
            await consumerDefinition.resultHandler(resultContext, e);
        }
    }

    stop() {
        if (!this.isRunning) {
            throw new Error('Consumer not running');
        }

        if (this.receiveRequest) {
            this.receiveRequest.abort();
        }

        this.isRunning = false;
    }
}