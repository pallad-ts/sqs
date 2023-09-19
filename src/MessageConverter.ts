import {DataType} from "./DataType";
import {SerializerInterface} from 'alpha-serializer';
import {Message} from "./Message";
import {Queue} from "./Queue";
import {SendMessageCommandInput, SendMessageBatchRequestEntry, Message as AWSMessage, MessageAttributeValue} from '@aws-sdk/client-sqs';
import * as is from 'predicates';

function isMessageAttribute(value: any): value is Message.Input.MessageAttribute {
	return is.all(
		is.prop('type', String),
		is.hasProperty('value')
	)(value);
}

export type CommonRawMessageFields = 'MessageBody' | 'MessageGroupId' | 'MessageAttributes' | 'DelaySeconds' | 'MessageDeduplicationId';
export type RawMessage = Pick<SendMessageCommandInput, CommonRawMessageFields>;
export type RawBatchMessage = Pick<SendMessageBatchRequestEntry, CommonRawMessageFields | 'Id'>;

export class MessageConverter {
	private dataTypes = new Map<string, DataType<any, any>>();

	constructor(private serializer: SerializerInterface<string>) {
		this.registerDataType(DataType.Common.STRING);
		this.registerDataType(DataType.Common.NUMBER);
		this.registerDataType(DataType.Common.BINARY);
	}

	registerDataType(type: DataType<any, any>): this {
		if (this.dataTypes.has(type.name)) {
			throw new Error(`Data type ${type.name} already exists`);
		}
		this.dataTypes.set(type.name, type);
		return this;
	}

	fromRawMessage<TBody = string, TAttributes extends Record<string, any> = Record<string, unknown>>(raw: AWSMessage, queue: Queue.Info): Message<TBody, TAttributes> {
		return new Message<TBody, TAttributes>(
			raw,
			queue,
			raw.Body ? this.serializer.deserialize(raw.Body) : '',
			this.messageAttributesFromRawMessage<TAttributes>(raw)
		)
	}

	private messageAttributesFromRawMessage<TAttributes extends Message.Attributes>(raw: AWSMessage): TAttributes {
		const result: any = {};
		if (raw.MessageAttributes) {
			for (const [key, value] of Object.entries(raw.MessageAttributes)) {
				result[key] = this.findDataType(value?.DataType ?? DataType.Common.STRING.name)
					.fromRaw(value);
			}
		}
		return result;
	}

	private findDataType(type: string): DataType<any, any> {
		if (this.dataTypes.has(type)) {
			return this.dataTypes.get(type) as DataType<any, any>;
		}
		return this.dataTypes.get(DataType.getBasicType(type)) as DataType<any, any>;
	}

	toRawMessage(input: Message.BatchInput): RawBatchMessage;
	toRawMessage(input: Message.Input): RawMessage;
	toRawMessage(input: Message.Input | Message.BatchInput): RawMessage | RawBatchMessage {
		const message: RawMessage = {
			MessageBody: this.serializer.serialize(input.body),
			MessageGroupId: input.groupId,
			MessageAttributes: input.attributes && this.messageAttributesFromInput(input.attributes),
			DelaySeconds: input.delay,
			MessageDeduplicationId: input.deduplicationId
		};

		if ('id' in input) {
			(message as RawBatchMessage).Id = input.id;
			return message as RawBatchMessage;
		}
		return message;
	}

	private messageAttributesFromInput(messageAttributes: NonNullable<Message.Input['attributes']>): Record<string, MessageAttributeValue> {
		const result: Record<string, MessageAttributeValue> = {};
		for (const [key, value] of Object.entries(messageAttributes)) {
			result[key] = this.attributeToRaw(value);
		}
		return result;

	}

	private attributeToRaw(value: Message.Input.MessageAttributeValue): MessageAttributeValue {

		for (const dataType of this.dataTypes.values()) {
			if (dataType.isType(value)) {
				return dataType.toRaw(value);
			}
		}

		if (isMessageAttribute(value)) {
			return this.findDataType(value.type)
				.toRaw(value.value);
		}

		throw new Error(`Value "${value}" is not serializable. Please use string, number, Buffer, define type explicitly or define data type that is able to serialize the value`);
	}
}
