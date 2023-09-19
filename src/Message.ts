import {TypedArray} from "./typedArray";
import {Queue} from "./Queue";
import {create} from "@pallad/id";
import {isBinaryKind} from "./DataType";
import {Message as AWSMessage} from '@aws-sdk/client-sqs';

export class Message<TBody = string, TAttributes extends Message.Attributes = Message.Attributes> {
	constructor(public readonly raw: AWSMessage,
				public readonly queue: Queue.Info,
				public readonly body: TBody,
				public readonly attributes: TAttributes) {
		Object.freeze(this);
	}

	get groupId(): string | undefined {
		return this.raw.Attributes?.MessageGroupId;
	}

	get deduplicationId(): string | undefined {
		return this.raw.Attributes?.MessageDeduplicationId;
	}

	get internalAttributes() {
		return this.raw.Attributes;
	}

	get sequenceNumber(): string | undefined {
		if (this.raw.Attributes) {
			return this.raw.Attributes.SequenceNumber as string;
		}
	}

	toInput(delay?: number): Message.Input {
		let attributes: { [key: string]: Message.Input.MessageAttribute } | undefined;

		if (this.raw.MessageAttributes) {
			attributes = {};
			for (const [key, value] of Object.entries(this.raw.MessageAttributes)) {
				const finalDataType = value.DataType ? value.DataType : 'String';
				attributes[key] = {
					type: finalDataType,
					value: isBinaryKind(finalDataType) ? value.BinaryValue : value.StringValue
				};
			}
		}
		return {
			body: this.body,
			attributes: attributes,
			deduplicationId: this.deduplicationId,
			groupId: this.groupId,
			delay
		};
	}
}

export namespace Message {
	export type Attributes = Record<string, unknown>

	export interface Input<TBody = any> {
		delay?: number;
		body: TBody;
		attributes?: { [key: string]: Input.MessageAttributeValue };
		groupId?: string;
		deduplicationId?: string;
	}

	export namespace Input {
		export function toBatchInput(input: Input) {
			return {
				...input,
				id: create()
			}
		}
	}

	export interface BatchInput<TBody = any> extends Input<TBody> {
		id: string;
	}

	export function isBatchInput(value: BatchInput | Input): value is BatchInput {
		return 'id' in value;
	}

	export namespace Input {
		export type MessageAttributeValue = string | number | Buffer | TypedArray | MessageAttribute;

		export interface MessageAttribute {
			type: string,
			value: any
		}
	}
}
