import mmap from "@prokopschield/mmap";
import fs from "fs";
import nsblob from "nsblob";
import { cacheFn } from "ps-std";

const MAGIC_SIZE = 8 * 7;

const SIZE_OFFSET = 7;
const ROOT_OFFSET = 8;
const FREE_OFFSET = 9;

const START_OFFSET = BigInt(1);
const CHUNK_SIZE = BigInt(80);

/** an internal data structure, don't use this */
export class Node {
	global_mapping: Buffer;
	data_buffer: Buffer;
	offset: bigint;

	constructor(mapping: Buffer, offset: bigint) {
		this.global_mapping = mapping;
		this.offset = offset;
		this.data_buffer = mapping.subarray(
			Number(CHUNK_SIZE * offset),
			Number(CHUNK_SIZE * (offset + BigInt(1)))
		);
	}

	get key() {
		return this.data_buffer.subarray(0, 32).toString("hex");
	}

	set key(key) {
		this.data_buffer.set(Buffer.from(key, "hex"), 0);
	}

	get value() {
		return this.data_buffer.subarray(32, 64).toString("hex");
	}

	set value(value) {
		this.data_buffer.set(Buffer.from(value, "hex"), 32);
	}

	get lptr() {
		return this.data_buffer.readBigUInt64LE(64);
	}

	set lptr(lptr) {
		this.data_buffer.writeBigUInt64LE(lptr, 64);
	}

	get rptr() {
		return this.data_buffer.readBigUInt64LE(72);
	}

	set rptr(rptr) {
		this.data_buffer.writeBigUInt64LE(rptr, 72);
	}
}

/** an internal data structure, don't use this */
export class HashMap {
	mapping: Buffer;
	mapu64: BigUint64Array;

	constructor(filename: string, size: number) {
		if (!fs.existsSync(filename)) {
			fs.writeFileSync(filename, "");
		}

		const stats = fs.statSync(filename);

		if (stats.size < size) {
			fs.truncateSync(filename, size);
		}

		this.mapping = mmap(filename, false);
		this.mapu64 = new BigUint64Array(this.mapping.buffer);

		if (!this.magic) {
			this.magic = `nscdn-hashmap\n${filename}\n${size}\n${new Date()}\n`;
		}

		if (this.size < BigInt(size)) {
			this.size = BigInt(size);
		}

		if (this.free < START_OFFSET) {
			this.free = START_OFFSET;
			this.root = this.free++;
		}
	}

	get magic() {
		return this.mapping
			.subarray(0, MAGIC_SIZE)
			.toString()
			.replace(/\0+/g, "");
	}

	set magic(magic) {
		this.mapping.set(Buffer.from(magic).subarray(0, MAGIC_SIZE), 0);
	}

	get size() {
		return this.mapu64[SIZE_OFFSET];
	}

	set size(size) {
		this.mapu64[SIZE_OFFSET] = size;
	}

	get root() {
		return this.mapu64[ROOT_OFFSET];
	}

	set root(root) {
		this.mapu64[ROOT_OFFSET] = root;
	}

	get free() {
		return this.mapu64[FREE_OFFSET];
	}

	set free(free) {
		this.mapu64[FREE_OFFSET] = free;
	}

	getNode = cacheFn((offset: bigint) => new Node(this.mapping, offset));

	get(key: string): string | undefined {
		let node = this.getNode(this.root);

		while (key !== node.key) {
			if (key < node.key) {
				if (node.lptr) {
					node = this.getNode(node.lptr);
				} else {
					return undefined;
				}
			} else {
				if (node.rptr) {
					node = this.getNode(node.rptr);
				} else {
					return undefined;
				}
			}
		}

		return node.value;
	}

	createNode(key: string) {
		let node = this.getNode(this.free++);

		node.key = key;

		return node;
	}

	set(key: string, value: string) {
		let node = this.getNode(this.root);

		while (node.key !== key) {
			if (node.key === "00000000000000000000000000000000") {
				node.key = key;
			} else if (key < node.key) {
				if (node.lptr) {
					node = this.getNode(node.lptr);
				} else {
					const new_node = this.createNode(key);

					node.lptr = new_node.offset;
					node = new_node;
				}
			} else {
				if (node.rptr) {
					node = this.getNode(node.rptr);
				} else {
					const new_node = this.createNode(key);

					node.rptr = new_node.offset;
					node = new_node;
				}
			}
		}

		node.value = value;
	}
}

export async function download<
	T extends string | number | boolean | T[] | Record<string, T> | null
>(hash: string): Promise<T | string> {
	if (typeof hash === "string" && /^[0-9a-f]{64}$/g.test(hash)) {
		const data = await nsblob.fetch_json<T>(hash);

		if (data !== "") {
			return data;
		} else if (
			hash ===
			/** empty string **/
			"1286f4d583fcffb2849e654c9ec78d95c03e83bf73d6b4c18b21df1dda49c7fa"
		) {
			return "";
		}
	}

	return hash;
}

export async function upload(data: any): Promise<string> {
	if (typeof data === "string" && /^[0-9a-f]{64}$/g.test(data)) {
		return data;
	} else {
		return nsblob.store_json(data);
	}
}

export class DataMap<
	K extends string | number | boolean | V[] | Record<string, V> | null,
	V extends string | number | boolean | V[] | Record<string, V> | null
> {
	hashmap: HashMap;

	constructor(filename: string, size: number) {
		this.hashmap = new HashMap(filename, size);
	}

	async get(key: K): Promise<V | string> {
		const hash = this.hashmap.get(await upload(key));

		return hash ? await download<V>(hash) : "";
	}

	async set(key: K, value: V): Promise<this> {
		const [key_hash, value_hash] = await Promise.all([
			upload(key),
			upload(value),
		]);

		this.hashmap.set(key_hash, value_hash);

		return this;
	}
}

export default DataMap;
