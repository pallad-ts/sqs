import {Message} from "./Message";
import {RetryAttributes} from "./RetryTopology";
import * as AWS from 'aws-sdk';

export function isFifoQueue(queueNameOrUrl: string) {
    return /\.fifo$/.test(queueNameOrUrl);
}


function getMessageAttributeValue(message: Message, name: string) {
    const attr = message.getMessageAttribute(name);
    if (attr) {
        return attr.StringValue;
    }
    return ''
}

export function getAlphaAttributes(message: Message) {
    return {
        date: +getMessageAttributeValue(message, RetryAttributes.date) || 0,
        targetQueueUrl: getMessageAttributeValue(message, RetryAttributes.queueUrl),
        messageGroup: getMessageAttributeValue(message, RetryAttributes.messageGroup),
        retryAttempt: +getMessageAttributeValue(message, RetryAttributes.retryAttempt) || 0
    };
}

export function createDeduplicationIdForRetry(currentMessageDeduplicationId: string, retryAttempt: number) {
    return currentMessageDeduplicationId.replace(/_alphaRetry([0-9]+)/, '') + '_alphaRetry' + retryAttempt;
}

export function removeUnsupportedMessageAttributesValues(messageAttributes: AWS.SQS.MessageBodyAttributeMap) {
    Object.keys(messageAttributes)
        .forEach(key => {
            const attr = messageAttributes[key];
            if ('StringListValues' in attr) {
                delete attr.StringListValues;
            }
            if ('BinaryListValues' in attr) {
                delete attr.BinaryListValues;
            }
        })
}