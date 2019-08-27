import * as AWS from 'aws-sdk';
import {TypedArray} from "./typedArray";
import {Queue} from "./Queue";
import {ulid} from "ulid";
import {isBinaryKind} from "./DataType";

export class Message<TBody = string, TAttributes = Message.Attributes> {
    constructor(public readonly raw: AWS.SQS.Message,
                public readonly queue: Queue.Info,
                public readonly body: TBody,
                public readonly attributes: TAttributes) {
        Object.freeze(this);
    }

    get groupId(): string | undefined {
        return (this.raw.Attributes as AWS.SQS.MessageSystemAttributeMap).MessageGroupId;
    }

    get deduplicationId(): string | undefined {
        return (this.raw.Attributes as AWS.SQS.MessageSystemAttributeMap).MessageDeduplicationId;
    }

    get internalAttributes() {
        return this.raw.Attributes;
    }

    get sequenceNumber(): string | undefined {
        if (this.raw.Attributes) {
            return this.raw.Attributes['SequenceNumber'] as string;
        }
    }

    toInput(delay?: number): Message.Input {
        let attributes: { [key: string]: Message.Input.MessageAttribute } | undefined = undefined;

        if (this.raw.MessageAttributes) {
            attributes = {};
            for (const [key, value] of Object.entries(this.raw.MessageAttributes)) {
                attributes[key] = {
                    type: value.DataType,
                    value: isBinaryKind(value.DataType) ? value.BinaryValue : value.StringValue
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
    export type Attributes = { [key: string]: any };

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
                id: ulid()
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
        export type MessageAttribute = { type: string, value: any };
    }
}