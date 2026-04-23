package ra;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

// A streaming implementation of the theta join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the pair (a, b) satisfies a predicate theta.

public class ThetaJoin<A,B> implements Query<Or<A,B>,Pair<A,B>> {

	private final BiPredicate<A,B> theta;
	private List<A> leftItems;
	private List<B> rightItems;

	private ThetaJoin(BiPredicate<A,B> theta) {
		this.theta = theta;
	}

	public static <A,B> ThetaJoin<A,B> from(BiPredicate<A,B> theta) {
		return new ThetaJoin<>(theta);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		leftItems = new ArrayList<>();
		rightItems = new ArrayList<>();
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		if (item.isLeft()) {
			A left = item.getLeft();
			for (B right : rightItems) {
				if (theta.test(left, right)) {
					sink.next(Pair.from(left, right));
				}
			}
			leftItems.add(left);
		} else {
			B right = item.getRight();
			for (A left : leftItems) {
				if (theta.test(left, right)) {
					sink.next(Pair.from(left, right));
				}
			}
			rightItems.add(right);
		}
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		sink.end();
	}
	
}
