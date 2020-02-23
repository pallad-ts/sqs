import * as AWS from 'aws-sdk';
import * as is from 'predicates';
import {Manager} from "../src/Manager";
import {QueueManager} from "../src/QueueManager";
import {MessageConverter} from "../src/MessageConverter";
import {serializer} from "alpha-serializer";
import {Agent} from 'https';

function assertEnv(name: string) {
    is.assert(is.notBlank, `Env variable ${name} not defined`)(process.env[name]);
}

assertEnv('ACCESS_KEY_ID');
assertEnv('ACCESS_KEY_SECRET');
assertEnv('REGION');

const sqs = new AWS.SQS({
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.ACCESS_KEY_SECRET,
    region: process.env.REGION,
    httpOptions: {
        agent: new Agent({
            keepAlive: true,
            keepAliveMsecs: 60000
        })
    }
});

export const manager = new Manager(
    new QueueManager(sqs),
    new MessageConverter(serializer)
);