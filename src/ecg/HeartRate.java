package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;
import utils.Pair;

public class HeartRate {

	public static Query<Integer,Double> qIntervals() {
		return Q.pipeline(PeakDetection.qPeaks(),
			Q.sWindow2((a, b) -> (double)(b - a) * 1000.0 / 360.0));
	}

	public static Query<Integer,Double> qHeartRateAvg() {
		return Q.pipeline(qIntervals(), Q.foldAvg(), Q.map(avg -> 60000.0 / avg));
	}

	public static Query<Integer,Double> qSDNN() {
		return Q.pipeline(qIntervals(), Q.foldStdev());
	}

	public static Query<Integer,Double> qRMSSD() {
		return Q.pipeline(qIntervals(),
			Q.sWindow2((a, b) -> b - a),
			Q.map(d -> d * d),
			Q.foldAvg(),
			Q.map(Math::sqrt));
	}

	public static Query<Integer,Double> qPNN50() {
		return Q.pipeline(qIntervals(),
			Q.sWindow2((a, b) -> Math.abs(b - a)),
			Q.fold(Pair.from(0.0, 0.0),
				(acc, d) -> Pair.from(
					acc.getLeft() + (d > 50.0 ? 1.0 : 0.0),
					acc.getRight() + 1.0)),
			Q.map(p -> 100.0 * p.getLeft() / p.getRight()));
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for the Heart Rate *****");
		System.out.println("****************************************");
		System.out.println();

		System.out.println("***** Intervals *****");
		Q.execute(Data.ecgStream("100.csv"), qIntervals(), S.printer());
		System.out.println();

		System.out.println("***** Average heart rate *****");
		Q.execute(Data.ecgStream("100-all.csv"), qHeartRateAvg(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: SDNN *****");
		Q.execute(Data.ecgStream("100-all.csv"), qSDNN(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: RMSSD *****");
		Q.execute(Data.ecgStream("100-all.csv"), qRMSSD(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: pNN50 *****");
		Q.execute(Data.ecgStream("100-all.csv"), qPNN50(), S.printer());
		System.out.println();
	}

}
