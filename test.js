const AWS = require('aws-sdk');
const Blob = require('blob');
const sqs = new AWS.SQS({
    region: 'eu-west-1'
});

(async () => {
    const url = await sqs.getQueueUrl({QueueName: 'test-queue2.fifo'}).promise();

    await sqs.sendMessage({
        QueueUrl: url.QueueUrl,
        MessageAttributes: {
            buffer: {
                DataType: 'Binary',
                BinaryValue: Buffer.from('adsfadf', 'utf8')
            },
            typedArray: {
                DataType: 'Binary',
                BinaryValue: new Int8Array(8)
            },
            bufferSpecial: {
                DataType: 'Binary.special',
                BinaryValue: Buffer.from('i am snowflake', 'utf8')
            },
            string: {
                DataType: 'String',
                StringValue: 'some string'
            },
            stringAsBinary: {
                DataType: 'String',
                BinaryValue: Buffer.from('some string', 'utf8')
            },
            stringSpecial: {
                DataType: 'String.special',
                StringValue: 'some special string'
            },
            number: {
                DataType: 'Number',
                StringValue: '10'
            },
            numberSpecial: {
                DataType: 'Number.special',
                StringValue: '1230'
            }
        },
        MessageBody: 'some body',
        MessageGroupId: 'barany'
    }).promise();

    const message = sqs.receiveMessage({
        QueueUrl: url.QueueUrl,
        AttributeNames: ['All'],
        MessageAttributeNames: ['.*'],
        MaxNumberOfMessages: 2
    });

    const result = await message.promise();
    console.log(result.Messages && result.Messages[0], result.Messages && result.Messages[1]);
    // console.log(message, await message.promise());
    //
    // console.log(message.Messages);
})();