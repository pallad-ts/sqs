import * as AWS from 'aws-sdk';
import {TypedArray} from "./typedArray";

export class Message<TBody = string, TAttributes = Message.Attributes> {
    constructor(public readonly raw: AWS.SQS.Message,
                public readonly meta: Message.Meta,
                public readonly body: TBody,
                public readonly messageAttributes: TAttributes) {
        Object.freeze(this);
    }

    get messageGroupId(): string | undefined {
        if (this.raw.Attributes) {
            return this.raw.Attributes.MessageGroupId;
        }
    }

    get messageDeduplicationId(): string | undefined {
        if (this.raw.Attributes) {
            return this.raw.Attributes.MessageDeduplicationId;
        }
    }

    get attributes() {
        return this.raw.Attributes;
    }
}

export namespace Message {

    export interface Meta {
        queueName: string;
        queueURL: string;
    }

    export type Attributes = { [key: string]: any };

    export interface Input<TBody = any> {
        delay: number;
        body: TBody;
        messageAttributes?: { [key: string]: Input.MessageAttributeValue };
        messageGroupId?: string;
        messageDeduplicationId?: string;
    }

    export namespace Input {
        export type MessageAttributeValue = string | number | Buffer | TypedArray | MessageAttribute;
        export type MessageAttribute = { type: string, value: any };
    }
}