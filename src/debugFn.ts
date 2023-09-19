import debugModule = require('debug');

export function debugFn(suffix?: string) {
	return debugModule('pallad-sqs' + (suffix ? ':' + suffix : ''));
}
