import * as is from 'predicates';
import {serializer} from "alpha-serializer";
import {SQSClient} from "@aws-sdk/client-sqs";
import {Manager} from "@src/Manager";
import {QueueManager} from "@src/QueueManager";
import {MessageConverter} from "@src/MessageConverter";

function assertEnv(name: string) {
	is.assert(is.notBlank, `Env variable ${name} not defined`)(process.env[name]);
}

assertEnv('ACCESS_KEY_ID');
assertEnv('ACCESS_KEY_SECRET');
assertEnv('REGION');

export const sqs = new SQSClient({
	credentials: {
		accessKeyId: process.env.ACCESS_KEY_ID!,
		secretAccessKey: process.env.ACCESS_KEY_SECRET!,
	},
	region: process.env.REGION!,
});

export const manager = new Manager(
	new QueueManager(sqs),
	new MessageConverter(serializer)
);
