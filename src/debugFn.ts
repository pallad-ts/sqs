import debugModule = require('debug');

export function debugFn(suffix?: string) {
    return debugModule('alpha-sqs' + (suffix ? ':' + suffix : ''));
}