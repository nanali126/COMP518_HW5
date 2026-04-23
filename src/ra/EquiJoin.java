package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the equi-join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the equality predicate f(a) = g(b) holds.

public class EquiJoin<A,B,T> implements Query<Or<A,B>,Pair<A,B>> {

	private final Function<A,T> f;
	private final Function<B,T> g;
	private Map<T,List<A>> leftIndex;
	private Map<T,List<B>> rightIndex;

	private EquiJoin(Function<A,T> f, Function<B,T> g) {
		this.f = f;
		this.g = g;
	}

	public static <A,B,T> EquiJoin<A,B,T> from(Function<A,T> f, Function<B,T> g) {
		return new EquiJoin<>(f, g);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		leftIndex = new HashMap<>();
		rightIndex = new HashMap<>();
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		if (item.isLeft()) {
			A left = item.getLeft();
			T key = f.apply(left);
			for (B right : rightIndex.getOrDefault(key, List.of())) {
				sink.next(Pair.from(left, right));
			}
			leftIndex.computeIfAbsent(key, ignored -> new ArrayList<>()).add(left);
		} else {
			B right = item.getRight();
			T key = g.apply(right);
			for (A left : leftIndex.getOrDefault(key, List.of())) {
				sink.next(Pair.from(left, right));
			}
			rightIndex.computeIfAbsent(key, ignored -> new ArrayList<>()).add(right);
		}
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		sink.end();
	}
	
}
