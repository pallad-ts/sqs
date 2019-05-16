export interface RetryTopology {
    /**
     * Name of queue where retry messages are being stored
     */
    queueName: string;
    retryDelay: number;
}

export namespace RetryTopology {
    export const Attributes = {
        date: '_alpha_RetryAfter',
        queueUrl: '_alpha_QueueUrl',
        messageGroup: '_alpha_MessageGroup',
        retryAttempt: '_alpha_retryAttempt'
    };

    export interface Enchanted extends RetryTopology {
        queueUrl: string;
    }
}