import * as fixtures from '../fixtures';
import {Queue} from "@src/Queue";

describe('Queue', () => {
    const QUEUE_STANDARD = fixtures.queue.standard;
    const QUEUE_FIFO = fixtures.queue.fifo;

    describe('structure', () => {
        it('standard', () => {
            expect(QUEUE_STANDARD)
                .toHaveProperty('isFifo', false);
        });

        it('fifo', () => {
            expect(QUEUE_FIFO)
                .toHaveProperty('isFifo', true);
        })
    });

    it('ensuring queue name to be fifo name', () => {
        expect(Queue.ensureFifoName('standard'))
            .toEqual('standard.fifo');

        expect(Queue.ensureFifoName('fifo.fifo'))
            .toEqual('fifo.fifo');
    });
});