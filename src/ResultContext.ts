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
				private message: TMessage) {
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
}
