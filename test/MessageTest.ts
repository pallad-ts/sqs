import * as fixtures from '../fixtures';
import {Message} from "@src/Message";
import {SQS} from "aws-sdk";

describe('Message', () => {
    const MESSAGE_STANDARD = fixtures.message.regular;
    const MESSAGE_STANDARD_NO_ATTRIBUTES = fixtures.message.noAttributes;
    const MESSAGE_FIFO = fixtures.message.fifo;
    const MESSAGE_FIFO_DEDUP = fixtures.message.fifoDedup;

    describe('structure', () => {

        it('standard', () => {
            const message = MESSAGE_STANDARD;
            expect(message.groupId)
                .toBeUndefined();

            expect(message.deduplicationId)
                .toBeUndefined();

            expect(message.internalAttributes)
                .toEqual(message.raw.Attributes);
        });

        it('fifo', () => {
            const message = MESSAGE_FIFO;
            expect(message.groupId)
                .toEqual(
                    (message.raw.Attributes as SQS.MessageSystemAttributeMap).MessageGroupId
                );

            expect(message.groupId)
                .toBeDefined();

            expect(message.deduplicationId)
                .toBeUndefined();

            expect(message.internalAttributes)
                .toEqual(message.raw.Attributes);
        });

        it('fifo dedup', () => {
            const message = MESSAGE_FIFO_DEDUP;
            expect(message.groupId)
                .toEqual(
                    (message.raw.Attributes as SQS.MessageSystemAttributeMap).MessageGroupId
                );

            expect(message.groupId)
                .toBeDefined();

            expect(message.deduplicationId)
                .toEqual(
                    (message.raw.Attributes as SQS.MessageSystemAttributeMap).MessageDeduplicationId
                );

            expect(message.deduplicationId)
                .toBeDefined();

            expect(message.internalAttributes)
                .toEqual(message.raw.Attributes);
        });
    });

    describe('converting to input', () => {
        describe.each<[string, Message<string, any>]>([
            ['standard', MESSAGE_STANDARD],
            ['standard without attributes', MESSAGE_STANDARD_NO_ATTRIBUTES],
            ['fifo', MESSAGE_FIFO],
            ['fifo with dedup', MESSAGE_FIFO_DEDUP]
        ])('message %s', (_s, message) => {
            it('with delay', () => {
                expect(message.toInput(100)).toMatchSnapshot()
            });

            it('without delay', () => {
                expect(message.toInput()).toMatchSnapshot();
            });
        });
    });

    describe('Input', () => {
        it('converting to batch input', () => {
            const input = MESSAGE_STANDARD.toInput();
            expect(Message.Input.toBatchInput(input))
                .toMatchObject({
                    ...input,
                    id: expect.stringMatching(/[A-Z0-9]{26}/)
                })
        });

        it('is batch input', () => {
            expect(Message.isBatchInput(MESSAGE_STANDARD.toInput()))
                .toBeFalsy();

            expect(Message.Input.toBatchInput(MESSAGE_STANDARD.toInput()))
                .toBeTruthy();
        });
    });
});