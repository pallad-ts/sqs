import * as AWS from 'aws-sdk';
import {Consumer} from "./Consumer";
import {Message} from "./Message";
import {Manager} from "./Manager";
import {ResultContext} from "./ResultContext";


const sqs = new AWS.SQS({region: 'eu-west-1'});

(async () => {
    const manager = new Manager(sqs);
    await manager.setupRetryTopology({
        queueName: 'retry_messages.fifo',
        retryDelay: 20000
    });

    await manager.consumeQueue({
        queueName: 'test-queue2.fifo',
        consumerFunction: () => {

            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    if (Math.random() > 0.4) {
                        resolve();
                    } else {
                        reject(new Error('test'));
                    }
                }, 2000);
            })

        },
        resultHandler: (context: ResultContext, error: any) => {
            if (error) {

                return context.retry();
            }
            return context.ack();
        }
    })
})();


process.on('unhandledRejection', (reason) => {
    console.log('rejected: ', reason);
});