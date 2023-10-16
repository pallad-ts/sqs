import {Message} from './Message';
import {Consumer} from "./Consumer";
import {ChangeMessageVisibilityCommand, DeleteMessageCommand, SQSClient} from "@aws-sdk/client-sqs";

/**
 * Type of function responsible for handling a consumer result.
 * Function MUST call one of ResultContext methods in order mark messages as rejected or acknowledge
 */
export type ResultHandler<TMessage extends Message<any, any>> = (context: ResultContext<TMessage>, error?: any, result?: any) => Promise<void>;

export class ResultContext<TMessage extends Message<any, any>> {

	constructor(private sqsClient: SQSClient,
				private consumer: Consumer<TMessage>,
				readonly message: TMessage) {
	}

	async ack() {
		await this.sqsClient.send(new DeleteMessageCommand({
			QueueUrl: this.message.queue.url,
			ReceiptHandle: this.message.raw.ReceiptHandle as string
		}))
		this.consumer.emit('consumed', this.message);
	}

	async reject() {
		await this.sqsClient.send(new ChangeMessageVisibilityCommand({
			QueueUrl: this.message.queue.url,
			ReceiptHandle: this.message.raw.ReceiptHandle as string,
			VisibilityTimeout: 0
		}));

		this.consumer.emit('rejected', this.message);
	}


	/**
	 * Delays retry of execution by changing message visibility time to elapsed visibility time + given delay
	 *
	 * After that delay message in queue will be visible again
	 */
	async delayedRetry(delayInSeconds: number) {
		if (delayInSeconds <= 0) {
			throw new Error('Delayed retry cannot be lower or equal 0');
		}
		const currentDateInSeconds = Math.floor(new Date().getTime() / 1000);
		const receiveDateInSeconds = Math.floor(this.message.receiveDate.getTime() / 1000);
		const elapsedTimeInSeconds = currentDateInSeconds - receiveDateInSeconds;
		await this.sqsClient.send(new ChangeMessageVisibilityCommand({
			QueueUrl: this.message.queue.url,
			ReceiptHandle: this.message.raw.ReceiptHandle as string,
			VisibilityTimeout: elapsedTimeInSeconds + delayInSeconds
		}));
	}
}
