import {DataType} from "@src/DataType";

describe('DataType', () => {
	describe('String', () => {
		const type = DataType.Common.STRING;

		const date = new Date();
		it.each<[any, string]>([
			['regular string', 'regular string'],
			[10, '10'],
			[date, String(date)]
		])('converting value to raw: %p', (source, expected) => {
			expect(type.toRaw(source))
				.toEqual({
					DataType: type.name,
					StringValue: expected
				});
		});

		it('converting raw to value', () => {
			const str = 'some value';
			expect(
				type.fromRaw({
					DataType: type.name,
					StringValue: str
				})
			)
				.toEqual(str);
		});

		it('is string type', () => {
			expect(type.isBinaryType)
				.toBeFalsy();

			expect(type.isStringType)
				.toBeTruthy();

			expect(type.isNumberType)
				.toBeFalsy();
		});

		it.each<[any, boolean]>([
			['test', true],
			[String('test'), true],
			[undefined, false],
			[10, false],
			[Buffer.from('test', 'utf8'), false]
		])('detects string type: %p', (value, expected) => {
			expect(type.isType(value))
				.toEqual(expected);
		});
	});

	describe('Number', () => {
		const type = DataType.Common.NUMBER;

		it.each<[any, string]>([
			[10, '10'],
			[1000.25, '1000.25'],
			['nan', 'nan']
		])('converting value to raw: %p', (source, expected) => {
			expect(type.toRaw(source))
				.toEqual({
					DataType: type.name,
					StringValue: expected
				});
		});

		it.each<[string, number]>([
			['10', 10],
			['1000.25', 1000.25],
			['nan', NaN]
		])('converting raw to value: %p', (raw, expected) => {
			expect(type.fromRaw({
				DataType: type.name,
				StringValue: raw
			}))
				.toEqual(expected);
		});

		it('is number type', () => {
			expect(type.isBinaryType)
				.toBeFalsy();

			expect(type.isStringType)
				.toBeFalsy();

			expect(type.isNumberType)
				.toBeTruthy();
		});

		it.each<[any, boolean]>([
			[10, true],
			[NaN, true],
			[undefined, false],
			['test', false],
			[Buffer.from('test', 'utf8'), false]
		])('detects number type: %p', (value, expected) => {
			expect(type.isType(value))
				.toEqual(expected);
		});
	});

	describe('Binary', () => {
		const type = DataType.Common.BINARY;

		const regularBuffer = Buffer.from('test', 'utf8');
		const uintArray = new Uint8Array([21, 31]);
		const string = 'some string';
		const stringBuffer = Buffer.from(string, 'utf8');
		it.each<any[]>([
			[regularBuffer, regularBuffer],
			[uintArray, uintArray],
			[string, stringBuffer]
		])('converting value to raw: %p', (source: any, expected: Buffer) => {
			expect(type.toRaw(source))
				.toStrictEqual({
					DataType: type.name,
					BinaryValue: expected
				});
		});

		it.each<any[]>([
			[regularBuffer, regularBuffer],
			[uintArray.buffer, uintArray.buffer],
			[stringBuffer, stringBuffer]
		])('converting raw to value: %p', (raw: any, expected: number) => {
			expect(type.fromRaw({
				DataType: type.name,
				BinaryValue: raw
			}))
				.toStrictEqual(expected);
		});

		it('is binary type', () => {
			expect(type.isBinaryType)
				.toBeTruthy();

			expect(type.isStringType)
				.toBeFalsy();

			expect(type.isNumberType)
				.toBeFalsy();
		});


		it('fails when converting unsupported value to raw', () => {
			expect(() => {
				type.toRaw(10 as any)
			})
				.toThrowError(/Value for binary attribute has to be/)
		});

		it.each<[any, boolean]>([
			['test', false],
			[10, false],
			[undefined, false],
			[stringBuffer, true],
			[uintArray, true],
		])('detects binary type: %p', (value, expected) => {
			expect(type.isType(value))
				.toEqual(expected);
		});
	});

	describe('Custom', () => {
		const type = new DataType<Date, string>(
			'String.date',
			x => x.toISOString(),
			x => new Date(x),
			x => x instanceof Date
		);

		const date = new Date();

		it('converting raw to value', () => {
			expect(
				type.fromRaw({
					DataType: type.name,
					StringValue: date.toISOString()
				})
			)
				.toEqual(date);
		});

		it('converting value to raw', () => {
			expect(type.toRaw(date))
				.toEqual({
					DataType: type.name,
					StringValue: date.toISOString()
				});
		});
	});

	it.each<[string, boolean]>([
		['String.test', true],
		['String.', false],
		['Number.test', true],
		['Number.', false],
		['Binary.test', true],
		['Binary.', false],
		['Fake.type', false]
	])('name needs to be prefixed with "String.", "Binary." or "Number.": %p', (type, isValid) => {
		const message = 'data type has to start with "String.", "Number." or "Binary."';

		const func = (x: any) => x;

		const e = expect(() => {
			return new DataType(type, func, func, x => true);
		});
		if (isValid) {
			e.not.toThrowError(message);
		} else {
			e.toThrowError(message);
		}
	});

	describe('getting basic type', () => {
		it.each<[string, string]>([
			['String.test', 'String'],
			['Number.test', 'Number'],
			['Binary.test', 'Binary']
		])('for: %p', (type, expected) => {
			expect(DataType.getBasicType(type))
				.toEqual(expected);
		});

		it('fails if prefix is not supported', () => {
			expect(() => {
				DataType.getBasicType('fake.type')
			})
				.toThrowError(/Cannot extract basic type from type/);
		})
	});
});
