import * as is from 'predicates';


export namespace Queue {

	export class Info {
		constructor(readonly name: string,
					readonly url: string,
					readonly attributes: Attributes) {
			Object.freeze(this);
		}

		get isFifo() {
			return isFifo(this.name);
		}

		get isStandard() {
			return !this.isFifo;
		}
	}

	export const isFifo = is.endsWith('.fifo');

	export interface Attributes {
		delay: number;
		maxMessageSize: number;
		retentionPeriod: number;
		receiveMessageWaitTime: number;
		visibilityTimeout: number;
		isFifo: boolean;
		isContentBasedDeduplication: boolean;
		arn: string;
		redrivePolicy: RedrivePolicy | undefined
		redriveAllowPolicy: RedriveAllowPolicy | undefined
	}

	export type RedriveAllowPolicy = 'allowAll' | 'denyAll' | { sourceQueueArns: string[] };

	export interface RedrivePolicy {
		deadLetterQueueArn: string;
		maxReceiveCount: number;
	}

	export namespace RedrivePolicy {
		export interface Input {
			deadLetterQueueArn: string;
			maxReceiveCount?: number;
		}
	}

	export namespace Attributes {
		export type Input = Partial<Omit<Attributes, 'arn' | 'redrivePolicy'>> & {
			redrivePolicy?: RedrivePolicy.Input
		};
	}

	/**
	 * Make sure that queue name ends with ".fifo"
	 */
	export function ensureFifoName(name: string) {
		return is.endsWith('.fifo', name) ? name : name + '.fifo';
	}

	export function stripFifoName(name: string) {
		return name.replace(/\.fifo$/i, '');
	}
}
