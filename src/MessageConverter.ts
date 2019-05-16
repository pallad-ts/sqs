import {DataType} from "./DataType";
import {Serializer} from 'alpha-serializer';
import {SQS} from 'aws-sdk';
import {Message} from "./Message";

export class MessageConverter {
    private dataTypes: Map<string, DataType<any, any>> = new Map();


    constructor(private serializer: Serializer) {
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

    fromRawMessage<TBody = string, TAttributes = Message.Attributes>(raw: SQS.Message, meta: Message.Meta): Message<TBody, TAttributes> {
        return new Message<TBody, TAttributes>(
            raw,
            meta,
            this.serializer.serialize(raw.Body),
            this.messageAttributesFromRawMessage<TAttributes>(raw)
        )
    }

    messageAttributesFromRawMessage<TAttributes extends Message.Attributes>(raw: SQS.Message): TAttributes {
        const result: TAttributes = {} as TAttributes;
        if (raw.MessageAttributes) {
            for (const [key, value] of Object.entries(raw.MessageAttributes)) {
                result[key] = this.findDataType(value.DataType)
                    .fromRaw(value);
            }
        }
        return result;
    }

    findDataType(type: string): DataType<any, any> {
        if (this.dataTypes.has(type)) {
            return this.dataTypes.get(type) as DataType<any, any>;
        }
        return this.dataTypes.get(DataType.getBasicType(type)) as DataType<any, any>;
    }

    toRawMessage(input: Message.Input): any {

    }
}