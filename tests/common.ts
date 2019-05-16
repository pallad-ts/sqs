import * as AWS from 'aws-sdk';
import * as sinon from 'sinon';

export function awsResult<T>(someResult: T): AWS.Request<T, AWS.AWSError> {
    const result = sinon.createStubInstance(AWS.Request);
    result.promise.resolves(someResult);
    return result;
}

