package ecg;

import dsl.Query;
import dsl.Sink;

public class Detect implements Query<VTL,Long> {

	private static final double THRESHOLD = 255.306;

	private boolean searching;
	private int searchCountdown;
	private int bestValue;
	private long bestTimestamp;
	private int refractoryCountdown;

	public Detect() {
	}

	@Override
	public void start(Sink<Long> sink) {
		searching = false;
		searchCountdown = 0;
		bestValue = Integer.MIN_VALUE;
		bestTimestamp = -1;
		refractoryCountdown = 0;
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		if (refractoryCountdown > 0) {
			refractoryCountdown--;
			return;
		}
		if (searching) {
			if (item.v > bestValue) {
				bestValue = item.v;
				bestTimestamp = item.ts;
			}
			searchCountdown--;
			if (searchCountdown == 0) {
				sink.next(bestTimestamp);
				searching = false;
				refractoryCountdown = 72;
			}
			return;
		}
		if (item.l > THRESHOLD) {
			searching = true;
			searchCountdown = 39;
			bestValue = item.v;
			bestTimestamp = item.ts;
		}
	}

	@Override
	public void end(Sink<Long> sink) {
		if (searching) {
			sink.next(bestTimestamp);
		}
		sink.end();
	}

}
