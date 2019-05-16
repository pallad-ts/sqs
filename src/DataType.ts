import * as is from 'predicates';
import {SQS} from 'aws-sdk';
import {isTypedArray, TypedArray} from "./typedArray";

const isStringKind = is.matches(/^String(?:\..+)?$/);
const isNumberKind = is.matches(/^Number(?:\..+)?$/);
const isBinaryKind = is.matches(/^Binary(?:\..+)?$/);

const assertName = is.assert(
    is.any(
        isStringKind,
        isNumberKind,
        isBinaryKind
    ),
    'data type has to start with "String.", "Number." or "Binary."'
);

export class DataType<TSource, TRaw extends DataType.RawType = DataType.RawType> {
    constructor(readonly name: string,
                private serializer: DataType.Serializer<TSource, TRaw>,
                private deserializer: DataType.Deserializer<TSource, TRaw>
    ) {
        assertName(this.name);
        Object.freeze(this);
    }

    get isStringType() {
        return isStringKind(this.name);
    }

    get isNumberType() {
        return isNumberKind(this.name);
    }

    get isBinaryType() {
        return isBinaryKind(this.name);
    }

    toRaw(value: TSource): SQS.MessageAttributeValue {
        const storageValue = this.serializer(value);

        const result: SQS.MessageAttributeValue = {
            DataType: this.name
        };

        if (this.isBinaryType) {
            result.BinaryValue = storageValue;
        } else {
            result.StringValue = storageValue as string;
        }
        return result;
    }

    fromRaw(value: SQS.MessageAttributeValue) {
        if (this.isBinaryType) {
            return this.deserializer(value.BinaryValue as TRaw);
        } else {
            return this.deserializer(value.StringValue as TRaw);
        }
    }
}

export namespace DataType {

    export function getBasicType(type: string) {
        if (isStringKind(type)) {
            return Common.STRING.name;
        }

        if (isNumberKind(type)) {
            return Common.NUMBER.name;
        }

        if (isBinaryKind(type)) {
            return Common.BINARY.name;
        }

        throw new Error(`Cannot extract basic type from type: ${type} as it has to be prefixed with "String.", "Number." or "Binary."`);
    }


    export type RawType = string | number | Buffer;

    export type Serializer<TSource, TRaw extends RawType = RawType> = (data: TSource) => TRaw;
    export type Deserializer<TSource, TRaw extends RawType = RawType> = (data: TRaw) => TSource;

    export namespace Common {
        export const STRING = new DataType<string, string>(
            'String',
            x => x + '',
            x => x
        );

        export const NUMBER = new DataType<number, string>(
            'Number',
            x => x + '',
            x => parseFloat(x)
        );

        export const BINARY = new DataType<Buffer | TypedArray | string, Buffer>(
            'Binary',
            (data) => {
                if (Buffer.isBuffer(data)) {
                    return data;
                }

                if (isTypedArray(data)) {
                    // treat it as buffer since aws-sdk is able to handle TypedArray
                    return data as Buffer;
                }

                if (is.string(data)) {
                    return Buffer.from(data, 'utf8');
                }

                throw new Error('Value for binary attribute has to be a Buffer, a TypedArray or a string');
            },
            x => x
        );
    }
}

