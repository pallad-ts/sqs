import * as is from 'predicates';

export type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | Uint8ClampedArray | Float32Array | Float64Array;

export function isTypedArray(value: any): value is TypedArray {
	return is.any(
		is.instanceOf(Int8Array),
		is.instanceOf(Uint8Array),
		is.instanceOf(Int16Array),
		is.instanceOf(Uint16Array),
		is.instanceOf(Int32Array),
		is.instanceOf(Uint32Array),
		is.instanceOf(Uint8ClampedArray),
		is.instanceOf(Float32Array),
		is.instanceOf(Float64Array)
	)(value);
}
