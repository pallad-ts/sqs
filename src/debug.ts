import * as debug from 'debug';

export default function (suffix?: string) {
    return debug('alpha-sqs' + (suffix ? ':' + suffix : ''));
}