package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.functions.FilterFunction;

public class NotFilterFunction<T> implements FilterFunction<T> {
	private static final long serialVersionUID = -2109562093871155005L;

	private final FilterFunction<T> inner;

	public NotFilterFunction(final FilterFunction<T> inner) {
		this.inner = inner;
	}

	@Override
	public boolean filter(T value) throws Exception {
		return !inner.filter(value);
	}
}
